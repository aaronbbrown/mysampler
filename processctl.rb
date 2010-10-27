class ProcessCtl
  STARTCMD, STOPCMD, STATUSCMD = 0,1,2

  attr_accessor :pidfile, :daemonize

  def initialize
    @pidfile = ""
    @daemonize = false
  end

  def start
    size = get_running_pids.size
    if size > 0
      puts "Daemon is already running"
      return 1
    end

#    Daemonize.daemonize if @daemonize
    if @daemonize
      #http://stackoverflow.com/questions/1740308/create-a-daemon-with-double-fork-in-ruby
      raise 'First fork failed' if (pid = fork) == -1
      exit unless pid.nil?

      Process.setsid
      raise 'Second fork failed' if (pid = fork) == -1
      exit unless pid.nil?

      Dir.chdir '/'
      File.umask 0000
      STDIN.reopen '/dev/null'
      STDOUT.reopen '/dev/null', 'a'
      STDERR.reopen STDOUT
    end
    write_pid unless pidfile == ""
    yield
    return 0
  end

  def stop
    get_running_pids.each do |pid|
      puts "Killing pid #{pid}"
      Process.kill :SIGTERM, pid
    end
    File.delete(@pidfile) if File.file?(@pidfile)
    return 0
  end
  # returns the exit status (1 if not running, 0 if running)
  def status
    size = get_running_pids.size
    puts "#{File.basename $0} is #{"not " if size < 1}running."
    return (size > 0) ? 0 : 1
  end

protected
  def write_pid
    File.open(@pidfile, "w") do |f|
#      f.write($$)
      f.write(Process.pid)
    end
  end

  def get_running_pids
    result = []
    if File.file? @pidfile
      pid = File.read @pidfile
      # big long line I stole to kill a pid
      result =  `ps -p #{pid} -o pid h`.to_a.map!{|s| s.to_i}
    end
    return result
  end
end
