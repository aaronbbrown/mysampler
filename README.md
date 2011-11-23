MySampler
=========

What is it?
-----------
MySampler is a tool written in ruby to poll SHOW GLOBAL STATUS in MySQL and output the values to either a CSV or graphite/carbon.
The interval at which the polling occurs can be specified and the output can be either the absolute or relative values, so you can see change over time.
If logging to CSV, the a date stamp is appended to the CSV file and it is rotated hourly (to be configurable later).

Dependencies
------------

MySampler requires the following gems:

graphite
sequel

Installation
------------
To install, simply install the dependencies above, and clone the repository and run mysample.rb

    gem install sequel
    gem install graphite
    git clone https://github.com/9minutesnooze/mysampler.git
    cd mysampler
    ./mysample.rb -o csv -u me -p secret -H localhost -f /tmp/mysample.csv -i 10 -r -d -k start

Options
-------
    Usage ./mysample.rb [OPTIONS]
    -u, --user USER                  MySQL User
    -p, --pass PASSWORD              MySQL Password
    -P, --port PORT                  MySQL port (default 3306)
        --pidfile PIDFILE            PID File (default: `pwd`/mysample.pid)
    -H, --host HOST                  MySQL hostname (default: localhost)
    -f, --file FILENAME              output filename (will be appended with rotation timestamp)
    -o, --output (csv|graphite)      Output format (default: csv)
    -i, --sleep SECONDS              Interval between runs (default: 10)
    -r, --relative                   Show the difference between the current and previous values (default: false)
    -d, --daemonize                  daemonize process (default: false)
    -k (start|stop|status)           command to pass daemon
        --command
    -g, --graphite HOST:PORT         Graphite server:port
    -h, --help                       this message

If daemonized with -d, currently STDERR/STDOUT does not go anywhere, so if you are having problems, try running it without the -d flag initially.

Caveats
-------
This project, while it runs in production, is rapidly changing so the command line parameters and output are not set in stone.  
I will attempt to write release notes if something changes drastically.
Currently a lot of the object structure is being revamped and I am adding more features such as SHOW MUTEX STATUS, and SHOW ENGINE INNODB STATUS.  
Those features are initially available in the class_refactor branch.
