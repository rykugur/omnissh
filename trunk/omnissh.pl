#!/usr/bin/perl5.10.0 -W

##
## OmniSSH 1.0rc1 -- SSH Cluster(-like) Management Tool by M. N. Busigin <mbusigin@gmail.com>
## Copyright 2009-2010 COMAND Solutions, Inc.
## Portions Copyright 2010, Matt Busigin
##     
## This is a program which is used to execute one command on many servers in a cluster
## in a parallel, reliable and well documented fashion.
##
## @ChangeLog:  Jul 17/09:  Started
##              Jul 20/09:  Continuing prototype
##              Jul 21/09:  -Detail flag;  Threading improvements
##              Jul 22/09:  Correctly interpreting ssh return code
##              Jul 27/09:  Added -stop;  Groundwork for -upload
##              Jul 28/09:  Finished -upload;  Added better parameter handling code to expose -maxthreads
##              Apr 27/10:  Preparing for initial open-source release
## 


##
## TODO:
##
#     * Clean up perl script
#     ! Display "n out of n" running
#     ! Add stop option
#     ! Add -upload file
#     ! Add -report (output all results for job x into a file or files)
#     * Add -force (works around default of hassle)
#     ! Add -series (does not do threaded and asks confirmation with yes/no/yes all/no all options)
#     * Assets DB integration for hosts
#     * -stop updates the status as such
#     * Filter stderr out from ssh connections complaining about this or that
#     * Make output far cooler
##
##

##
## Design overview:
##
## A job is a collection of commands sent to different servers.
## A command is an individual string which will be executed on the shell of a server through ssh.
##
## When you start a job, the following things happen:
##
##      1. The job is saved to the database
##      2. The job is split up into commands -- one for each server that the command is run on
##         and saved to the database
##      3. The tool forks so the user returns to the command prompt
##      4. For each command, a new thread is spawned to execute the command with Net::SSH::Expect
##
## After the main thread has spun off all of the new threads, it waits on a Thread::Queue, and 
## maintains current status of the job and the commands in the database.  Consequently, further
## tool invocations gain information about what's running by simply looking into the database.
##



use strict;
use DBI;
use Data::Dumper;
use OmniSSH;
use Term::ANSIColor;


my %statuscodes =
(
    '0'     => 'Ready',
    '1'     => 'Connecting',
    '2'     => 'Running',
    '3'     => 'Finalising',
    '4'     => 'Finished',
    '-1'    => 'Failed',
    '-2',   => 'Stopped',
);

my %globalstatuscodes =
(
    '0'     => 'Stop',
    '1'     => 'Go',
);

my %argument_parameters =
(
    '-maxthreads'       => 1,
    '-serial'           => 0,
    '-identity'		=> 1,
);

my $USAGE = <<EOF

OmniSSH 1.0rc1 -- SSH Cluster(-like) Management Tool by M. N. Busigin <mbusigin\@gmail.com>
Copyright 2009-2010 COMAND Solutions, Inc.
Portions Copyright 2010, Matt Busigin

This is a program which is used to execute one command or upload files on many servers in a 
cluster in a parallel, reliable and well documented fashion.

Usage:  

Start new job:                   omnissh.pl [...options...] <host1>[ <host2> ...] -e [<command>]
Show running and completed jobs: omnissh.pl -show [<JID>]
Display detail on command:       omnissh.pl -detail <CID>
Upload a file:                   omnissh.pl -upload <local path> <remote path> <host1>[ <host2> ...]
Stop spinning up new threads:    omnissh.pl -stop
Report results of jobs:          omnissh.pl -report <JID> <directory>

Valid options for starting a new ssh job (-e):

        -maxthreads <n>          Spin up a maximum of N threads
        -serial                  Do one command or upload at a time, prompting after each time
        
    
EOF
;

my $dbh = connect_dbh();
my $config_hr = parse_arguments( \@ARGV )
    if @ARGV != 0;
if ( @ARGV == 0 || $config_hr->{'error'} )
{
    print $USAGE; 
    exit 1;
}

if ( $config_hr->{'new'} )
{
    my $jid = OmniSSH::new_ssh_job( $dbh, $config_hr );   
    OmniSSH::execute_job( $dbh, $jid, $config_hr );
}
elsif ( $config_hr->{'upload'} )
{
    my $jid = OmniSSH::new_scp_job( $dbh, $config_hr );
    OmniSSH::execute_job( $dbh, $jid, $config_hr );
}
elsif ( $config_hr->{'show'} )
{
    if ( $config_hr->{'jid'} )
    {
        show_commands( $dbh, $config_hr->{jid}, $config_hr );
    }
    else
    {
        show_jobs( $dbh );
    }
}
elsif ( $config_hr->{'details'} )
{
    show_details( $dbh, $config_hr->{'cid'} );
}
elsif ( $config_hr->{'stop'} )
{
    print "Setting stop flag!\n";
    $dbh->do( 'insert into STATUS values( null, DATETIME(\'NOW\'), 0 )' );
}
elsif ( $config_hr->{'report'} )
{
    report_on_job( $dbh, $config_hr );
}


##
## Display details for a job
##
sub show_details
{
    my $dbh = shift;
    my $cid = shift;
    
    my $command = $dbh->selectrow_hashref( 
        'select * from COMMANDS, SSH where COMMANDS.ID = SSH.COMMAND_ID and COMMANDS.ID = ?', 
        undef, ($cid) );
    
    foreach my $k (keys %{$command})
    {
        printf( "%-20s : %s\n", $k, $command->{$k} )
                    if ( $k ne 'RESULT' );
    }
    
    print "\nResult:\n";
    $command->{RESULT} ||= '';
    print $command->{RESULT} . "\n";
}



##
## Display running & run commands for a job
##
sub show_commands
{
    my $dbh = shift;
    my $jid = shift;
    
    printf( "%4s %4s %-20s %-12s %20s %-20s %s\n",
            'CID',
            'PID',
            'HOST',
            'STATUS',
            'STARTED',
            'FINISHED',
            'COMMAND'
            );
    
    my $commands = $dbh->selectall_hashref( 'select * from COMMANDS,SSH where COMMAND_ID = COMMANDS.ID and JOB_ID = ? order by STATUS', 'COMMAND_ID', undef, ($jid) );
    foreach my $cid (keys %{$commands})
    {       
        my $command = $commands->{ $cid };
        $command->{ 'ID' } = $cid;
        printf( "%4s %4s %-20s %-12s %20s %-20s %s\n",
                $command->{'ID'},
                $command->{'PID'} || '',
                $command->{'HOST'},
                $statuscodes{ $command->{'STATUS'} },
                $command->{'STARTED'} || '(not started)',
                $command->{'FINISHED'} || '(not finished)',
                $command->{'COMMAND'} );
    }

    $commands = $dbh->selectall_hashref( 'select * from COMMANDS,SCP where COMMAND_ID = COMMANDS.ID and JOB_ID = ? order by STATUS', 'COMMAND_ID', undef, ($jid) );
    foreach my $cid (keys %{$commands})
    {       
        my $command = $commands->{ $cid };
        $command->{ 'ID' } = $cid;
        printf( "%4s %4s %-20s %-12s %20s %-20s %s\n",
                $command->{'ID'},
                $command->{'PID'} || '',
                $command->{'HOST'},
                $statuscodes{ $command->{'STATUS'} },
                $command->{'STARTED'} || '(not started)',
                $command->{'FINISHED'} || '(not finished)',
                $command->{'LOCAL_PATH'} );
    }
}

##
## Display a list of jobs and their status to standard output
##
sub show_jobs
{
    my $dbh = shift;
    
    printf( "%-5s %-10s %-7s %-7s %20s %-20s %s\n",
            'JID',
            'USER',
            'HOSTS',
            'FAILED',
            'STARTED',
            'FINISHED',
            'ACTION' );
    my $jobs = $dbh->selectall_arrayref( 'select * from JOBS order by STARTED' );
    foreach my $job (@{$jobs})
    {
        # Figure out
        #   a. When the last command from this job was executed
        #   b. If the job is done (all of the commands have been run)
        #   c. The command string that the job is running
        #   d. How many hosts we're running this on
        my $last_command_finished = $dbh->selectrow_array( 'select FINISHED from COMMANDS where JOB_ID = ? and STATUS = 5 order by FINISHED limit 1',
                                                            undef,
                                                            ($job->[0])
                                                         ) 
                                        ||
                                    '(running)'                                   
                                    ;
        
        my $command = $dbh->selectrow_array( 'select COMMAND from COMMANDS, SSH where SSH.COMMAND_ID = COMMANDS.ID and JOB_ID = ? limit 1', undef, ($job->[0]) )
                    || $dbh->selectrow_array( 'select LOCAL_PATH from COMMANDS, SCP where SCP.COMMAND_ID = COMMANDS.ID and JOB_ID = ? limit 1', undef, ($job->[0]) );
        
        my $hosts = $dbh->selectrow_array( 'select count(*) from COMMANDS where JOB_ID = ?', undef, ($job->[0]) );
        my $hosts_finished = $dbh->selectrow_array( 'select count(*) from COMMANDS where JOB_ID = ? and FINISHED NOT NULL', undef, ($job->[0]) );
        my $hosts_failed = $dbh->selectrow_array( 'select count(*) from COMMANDS where JOB_ID = ? and FINISHED NOT NULL and STATUS = -1', undef, ($job->[0]) ) || 0;        
        printf( "%-5d %-10s %3s/%-3s %-6s %20s %-20s %s\n", 
                $job->[0],
                $job->[1],
                $hosts,
                $hosts_finished,
                $hosts_failed,
                $job->[2],
                $job->[3] || '(running)',
                $command
               );
    }
}


##
## Parse command line arguments, and put the result into a hashref so it may easily be instantiated
##
## Usage:  omnissh <host1>[, <host2> ...] -e [<command>]
##
sub parse_arguments
{
    my $args_ar = pop;
    my @args = @$args_ar;
    my %config;
    my @hosts;
    
    
    ##
    ## Config defaults go here
    ##
    $config{ 'maxthreads' } = 30;
    
    
    if ( $args[0] eq '-stop' )
    {
        $config{ 'stop' } = 1;
        $config{ 'new' } = 0;
        $config{ 'show' } = 0;
        return \%config;
    }
    
    if ( $args[0] eq '-show' )
    {
        $config{ 'new' } = 0;
        $config{ 'show' } = 1;
        
        if ( @args > 1 )
        {
            $config{ 'jid' } = $args[1];
        }
        return \%config;
    }
    
    if ( $args[0] eq '-detail' )
    {
        $config{ 'new' } = 0;
        $config{ 'details' } = 1;

        if ( @args > 1 )
        {
            $config{ 'cid' } = $args[1];
        }
        else
        {
            warn "You need to pass a command ID.\n";
            exit 1;
        }
        
        return \%config;
    }
    

    if ( $args[0] eq '-upload' )
    {
        $config{ 'upload' } = 1;
        if ( $#args < 3 )
        {
            warn "Incorrect number of arguments to -upload\n";
            $config{ 'error' } = 1;
            return \%config;
        }
        

        shift @args;
        $config{ 'local' } = shift @args;
        $config{ 'remote' } = shift @args;

        my @hosts;
        while( (my $host = shift @args) )
        {
            push @hosts, $host;
        }
        $config{ 'hosts' } = \@hosts;

        return \%config;        
    }
    
    if ( $args[0] eq '-report' )
    {
        $config{ 'report' } = 1;
        if ( $#args < 2 )
        {
            warn "Incorrect number of arguments to -upload;  you need to specify both the job and a directory to report to.\n";
            $config{ 'error' } = 1;
        }
        else
        {
            $config{ 'jid' } = $args[ 1 ];
            $config{ 'reportdirectory' } = $args[ 2 ];
        }
        
        return \%config;        
    }    
    
    
    ##
    ## The default behaviour is to spin up an ssh job (-e).
    ##
    @args = reverse @args;
    
    # We start to make sure if the first arguments include flags
    while( (my $arg = pop @args) )
    {
        if ( $arg =~ /^-(.+)$/ )
        {
            my $flag = $1;
            my $val = 1;
            my $parameters = $argument_parameters{ $arg };
            if ( $parameters > 0 )
            {
                if ( $#args == -1 )
                {
                    warn "The '$arg' flag needs $parameters parameters\n";
                    $config{ 'error' } = 1;
                    return \%config;
                }
                $val = pop @args;
            }
            $config{ $flag } = $val;
        }
        else
        {
            push @args, $arg;
            goto doneargs;   
        }
    }

doneargs:
    
    # We then grab the hosts;  we stop at the -e
    while( (my $arg = pop @args) )
    {
        goto DoneHosts
            if ( $arg eq '-e' );

        push @hosts, $arg;
    }
DoneHosts:

    $config{ 'hosts' } = \@hosts;
    if ( @hosts == 0 )
    {
        warn "Error:  you need to pass at least one host.\n";
        $config{ 'error' } = 1;
    }
    
    # Next is the command.  We will concatenate multiple arguments here with a space for convenience sake.
    my $command = pop @args;
    if ( !$command )
    {
        print "Type in your command line (and send EOF with ctrl-d): ";
        $config{'command'} = '';
        while( <STDIN> )
        {
            $config{ 'command' } .= $_;
        }
    }
    else
    {
        # We want to facilitate commands being sent with multiple arguments without encapsulating 
        # them all in '' or ""
        while ( (my $next = pop @args) )
        {
            $command .= ' ' . $next;
        }
        $config{ 'command' } = $command;
    }
    
    
    if ( !exists($config{'error'}) )
    {
        $config{ 'error' } = 0;
    }
    
    $config{ 'new' } = 1;
#    warn Dumper( \%config );

    return \%config;
}


##
## Print out a full report on a job to a directory.  This involves a couple of things:
##
##      1. An HTML file with a 'pretty' report (which we'll use in conjunction with an HTTPd at some point)
##      2. A series of .txt files which are named after the command ID which include the text output of each command executed
##      3. A series of .status files which include a brief enumeration of status codes from each command executed
##
sub report_on_job
{
    my ( $dbh, $config_hr ) = @_;
    
    my $dir = $config_hr->{ 'reportdirectory' };
    my $jid = $config_hr->{ 'jid' };
    my $job = $dbh->selectrow_hashref( 'select * from JOBS where ID = ?', undef, ($config_hr->{'jid'}) );
    open( F, ">$dir/job.txt" ) or die "Couldn't open file $dir/job.txt\n";
    foreach my $k (keys %{$job})
    {
        print F "$k   $job->{$k}\n";
    }
    close F;
    
    my $commands = $dbh->selectall_hashref( 'select * from COMMANDS,SSH where COMMAND_ID = COMMANDS.ID and JOB_ID = ? order by STATUS', 'COMMAND_ID', undef, ($jid) );
    foreach my $cid (keys %{$commands})
    {       
        my $command = $commands->{ $cid };
        $command->{ 'ID' } = $cid;
        my $out = sprintf( "ID %-4s\nPID %-4s\nHOST %-20s\nSTATUS %-12s\nSTARTED %-20s\nFINISHED %-20s\nCOMMAND %s\n",
                $command->{'ID'},
                $command->{'PID'} || '',
                $command->{'HOST'},
                $statuscodes{ $command->{'STATUS'} },
                $command->{'STARTED'} || '(not started)',
                $command->{'FINISHED'} || '(not finished)',
                $command->{'COMMAND'} );
        open( F, ">$dir/$cid.status.txt" ) or die "Couldn't open file $dir/$cid.status.txt\n";
        print F $out;
        close( F );
        
        open( F, ">$dir/$cid.output.txt" ) or die "Couldn't open file $dir/$cid.output.txt\n";
        print F $command->{'RESULT'};
        close( F );
    }
}


##
## Connect to database;  Create tables if necessary;  Return handle.
##
sub connect_dbh
{
    my $dbh = DBI->connect( 'dbi:SQLite:dbname=' . $ENV{'HOME'} . '/omnissh.db', '', '' );


    my $jobs_table = <<EOF
        create table JOBS
        (
            ID integer primary key,
            NAME VARCHAR(64),
            STARTED DATESTAMP,
            FINISHED DATESTAMP
        );
EOF
;

    my $commands_table = <<EOF
        create table COMMANDS
        (
            ID integer primary key,
            JOB_ID int,
            HOST varchar(64),
            STARTED datestamp,
            FINISHED datestamp,
            STATUS int,
            PID int
        );
EOF
;

    my $ssh_table = <<EOF
        create table SSH
        (
            ID integer primary key,
            COMMAND_ID int,
            COMMAND text,
            RESULT text
        );
EOF
;

    my $scp_table = <<EOF
        create table SCP
        (
            ID integer primary key,
            COMMAND_ID int,
            LOCAL_PATH text,
            REMOTE_PATH text
        );
EOF
;
    
    
    my $status_table = <<EOF
           create table STATUS
           (
            ID integer primary key,
            MARK datestamp,
            STATUS int
           );
EOF
;

    my $hr = $dbh->selectall_hashref( 'select * from JOBS', 'ID' );
    if ( !$hr ) # Database apparently isn't there:  let's create the tables
    {
        warn "No database found:  creating the necessary tables..\n";
        $dbh->do( $jobs_table );
        $dbh->do( $commands_table );
        $dbh->do( $status_table );
        $dbh->do( $scp_table );
        $dbh->do( $ssh_table );
    }
    
    return $dbh;
}

END { system( 'stty sane' ); }
