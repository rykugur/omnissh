                                   OmniSSH  
                      OmniSSH - Introduction & Tutorial
Introduction

This is a Perl program which is used to execute one command on many servers
in a cluster in a parallel, reliable and well documented fashion.

Concepts & Design

A job is a collection of commands sent to different servers.

A command is an individual string which will be executed on the shell of a
server through ssh.

When you start a job, the following things happen:

 1. The job is saved to the database

 2. The job is split up into commands – one for each server that the command
    is run on and saved to the database

 3. Tool forks so the user returns to the command prompt

 4. For each command, a new thread is spawned to execute the command

 5. After the main thread has spun off all of the new threads, it waits for
    threads to report back to it, and maintains current status of the job
    and the commands in the database. Consequently, further tool invocations
    gain information about what's running by simply looking into the
    database.


Usage

OmniSSH 1.0rc1 -- SSH Cluster(-like) Management Tool by M. N. Busigin <mbusigin@gmail.com>
Copyright 2009-2010 COMAND Solutions, Inc.
Portions Copyright 2010, Matt Busigin

This is a program which is used to execute one command or upload files on
many servers in a cluster in a parallel, reliable and well documented
fashion.

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
Examples

To run an 'ls /home' on a group of servers, it's a very simple operation:

omnissh.pl server1 server2 -e ls /home

Everything after the -e – even the multiple arguments are concatenated with
spaces and sent as a single string to the server.

If everything has gone well, the command will return as the process forks,
and spins up the threads to do the work. To mitigate excessive resource
usage, the engine will use a maximum of 30 threads. It will wait until one
is finished before spinning up more to the maximum.

After you've fired off a job, you can view what jobs are running and have
recently run:

	omnissh.pl -show

This will return something like:

JID   USER       HOSTS              STARTED FINISHED             COMMAND
1     mbusigin   3      2009-07-21 17:45:34 2009-07-21 17:45:38  ls -l /
2     mbusigin   1      2009-07-21 18:42:38 2009-07-21 18:42:42  ls /
You can then view the details of a certain job by job ID (JID):

	omnissh.pl -show 1

This will give you a list of commands associated with this job, its status,
and when it started/finished:

CID PID HOST STATUS STARTED FINISHED COMMAND

 1      ford                 Finished      2009-07-21 17:45:35 2009-07-21 17:45:37  ls -l /
 2      ford                 Finished      2009-07-21 17:45:35 2009-07-21 17:45:36  ls -l /
 3      ford                 Finished      2009-07-21 17:45:35 2009-07-21 17:45:36  ls -l /

You can drill down one further step by looking at the complete details –
including the text of the output – with this command:

	omnissh.pl -detail 3

This will look at command ID (CID) 3:

ID                   : 3
PID                  : 0
HOST                 : ford
FINISHED             : 2009-07-21 17:45:36
STATUS               : 4
STARTED              : 2009-07-21 17:45:35
JOB_ID               : 1
COMMAND              : ls -l /
Result:
total 164
drwxr-xr-x   2 root root  4096 2009-06-19 13:52 bin
drwxr-xr-x   3 root root  4096 2009-05-26 23:05 boot
lrwxrwxrwx   1 root root    11 2008-05-07 06:13 cdrom -> media/cdrom
...
