package OmniSSH;

use strict;
use DBI;
use threads;
use threads::shared;
use Thread::Queue;
use Clone qw(clone);
#use Proc::Fork;
use Data::Dumper;
use Proc::SafePipe;
use Net::SCP;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
require Exporter;

@EXPORT = qw(execute_job new_job new_ssh_job new_scp_job);


##
## Execute a job in parallel
##
sub execute_job
{
    #
    # OK, so here's the situation:  the job has been broken down into commands, and everything
    # is saved to the database.  It's even possible that this is a second run.  All we know
    # is the the JID, and we have to run all of the commands associated with it in parallel.
    #
    # The other major component is that we want the user's interactive session to end here, 
    # so we've got to fork.
    #
    my ( $dbh, $jid, $config_hr ) = @_;
    
    $dbh->do( 'insert into STATUS values( null, DATETIME(\'NOW\'), 1 )' );
    my $job = $dbh->selectrow_hashref( 'select * from JOBS where ID = ?', undef, ($jid) );
    my $commands_hr = $dbh->selectall_hashref( 'select * from COMMANDS, SSH where SSH.COMMAND_ID = COMMANDS.ID and JOB_ID = ?', 'ID', undef, ($jid) );
    my $files_hr = $dbh->selectall_hashref( 'select * from COMMANDS, SCP where SCP.COMMAND_ID = COMMANDS.ID and JOB_ID = ?', 'ID', undef, ($jid) );        
    my $queue:shared = Thread::Queue->new() or die "Couldn't create Thread::Queue: $@\n";
    
    
    ##
    ## If we are not in serial mode (which we normally aren't), we want to fork.
    ##
    if ( !exists($config_hr->{'serial'}) )
    {
        run_fork
        {
            child
            {
                execute_job_engine( $dbh, $jid, $config_hr, $job, $commands_hr, $files_hr, $queue );
            }
            parent
            {
                my $child_pid = shift;
                # parent code goes here.
    #            waitpid $child_pid, 0;
                exit 0;
            }
            error
            {
                warn "For some reason, we can't fork: $@\n";
                exit 0;
            }
        };        
    }
    ##
    ## OK - we are in serial mode.
    ##
    else
    {
        warn "Not forking ...\n";
        execute_job_engine( $dbh, $jid, $config_hr, $job, $commands_hr, $files_hr, $queue );
    }
}


##
## The core of the execution of a job happens here.
##
sub execute_job_engine
{
    my ( $dbh, $jid, $config_hr, $job, $commands_hr, $files_hr, $queue ) = @_;
    
    my $serial = $config_hr->{'serial'} ? 1 : 0;
    my $prompt = $config_hr->{'y'} ? 0 : 1;
    
    my %commands = %$commands_hr;
    my %files = %$files_hr;
      
    my @threads;
    my @commands;
    my @files;
    foreach my $cid ( keys %commands )
    {
        push @commands, $cid;
        $commands{ $cid }{ 'ID' } = $cid;
    }
    foreach my $cid ( keys %files )
    {
        push @files, $cid;
        $files{ $cid }{ 'ID' } = $cid;
    }
    
    my $commands_left = $#commands + 1;
    my $files_left = $#files + 1;   
    
    while( 1 )
    {
        #
        # If the stop status is turned on, turn don't do anything
        #
        my $stop_flag = $dbh->selectrow_array( 'select STATUS from STATUS order by ID desc limit 1' );
        if ( $stop_flag == 1 )
        {
            # If we have threads to spare, spin some up!!
            # Pop them off the @commands stack, and push them onto the @threads stack.
            # Stop when we've rached maxthreads.
            if ( @threads < $config_hr->{maxthreads} )
            {
                while( (my $cid = pop @commands) )
                {
                    if ( $serial )
                    {
                        while( 1 )
                        {
                            goto doneprompting
                              if !$prompt;
                              
                            print   "Job ID $jid / Command ID $cid " . $commands{$cid}{'HOST'} . ': ' .
                                    $commands{$cid}{'COMMAND'} . "\n" .
                                    "(C)ontinue, Just do (A)ll of them, or (B)arf? : \n";
                            my $input = <STDIN>;
                            chop $input;
                            if ( $input =~ /^[Cc]$/ )
                            {
                                goto doneprompting;
                            }
                            elsif ( $input =~ /^[Aa]$/ )
                            {
                                $serial = 0;
                                goto doneprompting;
                            }
                            elsif ( $input =~ /^[bB]$/ )
                            {
                                goto doneallwork;
                            }
                        }
                    }


doneprompting:                    
                    my @arguments = ( $jid, $commands{$cid}, $config_hr, $queue );
                    my $thr = threads->new( \&ssh_worker_thread, @arguments );
                    push @threads, $thr;
                    
                    goto bailfromthreadcreation 
                        if ( @threads >= $config_hr->{maxthreads} );
                }

                while( (my $cid = pop @files) )
                {
                    my @arguments = ( $jid, $files{$cid}, $config_hr, $queue );
                    my $thr = threads->new( \&scp_worker_thread, @arguments );
                    push @threads, $thr;
                    
                    goto bailfromthreadcreation 
                        if ( @threads >= $config_hr->{maxthreads} );
                }
            }
        }
        else
        {
            # Pop them all off
            while( (my $cid = pop @commands) )
            {
                print "Setting command $cid status to stopped\n";
                $dbh->do( 'update COMMANDS set STATUS = -2 where ID = ?', undef, $cid );
            }
        }
        
    bailfromthreadcreation:

        my $ret = $queue->dequeue_nb;
        if ( $ret )
        {
            if ( $ret->[0] eq 'Start' )
            {
                $dbh->do( 'update COMMANDS set STARTED = DATETIME(\'NOW\') where ID = ?', undef, $ret->[2] );
            }
            elsif ( $ret->[0] eq 'Command Status' )
            {
                $dbh->do( 'update COMMANDS set STATUS = ? where ID = ?', undef, $ret->[3], $ret->[2] );
            }
            elsif ( $ret->[0] eq 'Record Output' )
            {
                $dbh->do( 'update SSH set RESULT = ? where COMMAND_ID = ?', undef, $ret->[3], $ret->[2] );
            }
            elsif ( $ret->[0] eq 'Stop' )
            {
                $dbh->do( 'update COMMANDS set FINISHED = DATETIME(\'NOW\') where ID = ?', undef, $ret->[2] );
                $commands_left --;
                $files_left --;
            }
            else
            {
                print "Unknown command $ret->[0]\n";
            }
        }
        
        my @newthreads;
        foreach my $thr (@threads)
        {
            if ($thr->is_joinable())
            {
                $thr->join();
            }
            else
            {
                push @newthreads, $thr;
            }
        }
        @threads = @newthreads;

        goto doneallwork
            if ( $#threads == -1 and $#commands == -1 and $commands_left < 1 and $#files == -1 and $files_left < 1 );
        
        # Sleep for 1/10th of a second - plain old sleep() doesn't work well with threads
        select( undef, undef, undef, 0.1 );
    }
    
    
doneallwork:
    $dbh->do( 'update JOBS set FINISHED = DATETIME(\'NOW\') where ID = ?', undef, ($jid) );       
    exit 0;

}



##
## Worker thread per ssh command
##
sub ssh_worker_thread
{
    my ( $jid, @args ) = @_;    
    my ( $command, $config_hr, $queue ) = @args;
    
    $queue->enqueue( ["Start", $jid, $command->{ID}] );
    $queue->enqueue( ["Command Status", $jid, $command->{ID}, 2] );

    my $identity = $config_hr->{ 'identity' };    
    my $ret;
#    if ( $identity )
#    {
#        open F, ">>/tmp/cfn-log.txt";
#        print F  'ssh', '-t', '-t', '-o', 'ConnectTimeout=4', '-o', 'StrictHostKeyChecking=no',  '-o', 'PasswordAuthentication=no', '-i', $identity, $command->{HOST}, $command->{COMMAND};
#close F;
#        $ret = backtick_noshell 'ssh', '-t', '-t', '-o', 'ConnectTimeout=4', '-o', 'StrictHostKeyChecking=no',  '-o', 'PasswordAuthentication=no', '-i', $identity, $command->{HOST}, $command->{COMMAND};
#    }
#    else
    {    
        $ret = backtick_noshell 'ssh', '-t', '-t', '-o', 'ConnectTimeout=4', '-o', 'StrictHostKeyChecking=no',  '-o', 'PasswordAuthentication=no', $command->{HOST}, $command->{COMMAND};
    }   
      
#    my $ret = `ssh -o StrictHostKeyChecking=no $command->{HOST} $command->{COMMAND}`;
    my $code = ($? >> 8);
    $queue->enqueue( ["Record Output", $jid, $command->{ID}, $ret] );
        
    if ( $code == 255 )
    {
        $queue->enqueue( ["Command Status", $jid, $command->{ID}, -1] );
    }
    else
    {
        $queue->enqueue( ["Command Status", $jid, $command->{ID}, 4] );
    }

    $queue->enqueue( ["Stop", $jid, $command->{ID}] );    
    
    sleep 2;
    return 0;
}


##
## Worker thread per scp file
##
sub scp_worker_thread
{
    my ( $jid, @args ) = @_;
    my ( $command, $config_hr, $queue ) = @args;
    
    my $user;
    my $host;
    if ( $command->{HOST} =~ /^([^@]+)@(.+)$/ )
    {
        $user = $1;
        $host = $2;
    }
    else
    {
        $user = `whoami`;
        chop $user;
        $host = $command->{HOST};
    }

    $queue->enqueue( ["Start", $jid, $command->{ID}] );

    my $tries = 0;
    
retryscp:
    $tries ++;
    my $scp = Net::SCP->new( {host => $host, user => $user} );
    warn "New: $scp\n";
    $queue->enqueue( ["Command Status", $jid, $command->{ID}, 1] );
    $queue->enqueue( ["Command Status", $jid, $command->{ID}, 2] );
    my $ret = $scp->put( $command->{LOCAL_PATH}, $command->{REMOTE_PATH} );
    warn "ret: $ret\n";
    
    if ( $ret == 0 )
    {
        warn "WTF: " .  $scp->{errstr} . "\n";
        if ( $scp->{errstr} =~ /lost connection/ )
        {
            goto retryscp
              if $tries < 3;
        }
        $queue->enqueue( ["Command Status", $jid, $command->{ID}, -1] );        
    }
    else
    {
        $queue->enqueue( ["Command Status", $jid, $command->{ID}, 3] );
        $ret = $scp->quit;    
        $queue->enqueue( ["Command Status", $jid, $command->{ID}, 4] );
    }

    $queue->enqueue( ["Stop", $jid, $command->{ID}] );    
    
    sleep 2;
    return 0;
}


##
## Spin up a new job
##
## This involves writing the new job and its commands to the database.
##
sub new_job
{
    my $dbh = shift;
    my $config = shift;
    my $me = `whoami`;
    chop( $me );
    $dbh->do( 'insert into JOBS values(NULL, ?, DATETIME(\'NOW\'), NULL)', undef,
                ($me) );
    my $job_id = $dbh->selectrow_array( 'SELECT last_insert_rowid()' );
    
    
    return $job_id;
}



##
## Do database preparation for new ssh job
##
sub new_ssh_job
{
    my $dbh = shift;
    my $config = shift;

    my $job_id = new_job( $dbh, $config );

    foreach my $host ( @{ $config->{hosts} } )
    {
        $dbh->do( 'insert into COMMANDS values( NULL, ?, ?, NULL, NULL, 0, 0 )', undef,
                    (
                        $job_id,
                        $host,
                    )
                );

        my $cid = $dbh->selectrow_array( 'select last_insert_rowid()' );
        $dbh->do( 'insert into SSH values( NULL, ?, ?, \'\' )', undef,
                    (
                        $cid,
                        $config->{ command },
                    )
                );
    }
    
    return $job_id;
}

##
## Do database preparation for new scp job
##
sub new_scp_job
{
    my $dbh = shift;
    my $config = shift;

    my $job_id = new_job( $dbh, $config );
    foreach my $host ( @{ $config->{hosts} } )
    {
        $dbh->do( 'insert into COMMANDS values( NULL, ?, ?, NULL, NULL, 0, 0 )', undef,
                    (
                        $job_id,
                        $host,
                    )
                );

        my $cid = $dbh->selectrow_array( 'select last_insert_rowid()' );
        $dbh->do( 'insert into SCP values( NULL, ?, ?, ? )', undef,
                    (
                        $cid,
                        $config->{ 'local' },
                        $config->{ 'remote' },
                    )
                );
    }
    
    return $job_id;    
}

my $testVAR1 = {
          'hosts' => [
                       'ford'
                     ],
          'maxthreads' => 10,
          'error' => 0,
          'new' => 1,
          'command' => 'whoami'
        };

sub prepare_ssh
{
    my ( @hosts, $maxthreads, $command ) = @_;
    my %ret;
    
    $ret{ 'hosts' } = \@hosts;
    $ret{ 'maxthreads' } = $maxthreads;
    $ret{ 'command' } = $command;
    $ret{ 'new' } = 1;
    $ret{ 'error' } = 0;

    return \%ret;
}


1;
