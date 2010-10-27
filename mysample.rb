#!/usr/bin/ruby

require 'optparse'
require 'processctl.rb'
require 'rubygems'
require 'dbi'

# This fixes a bug in the library where an unknown type will cause
# exceptions to be raised, rather than using a reasonable default: String
# coercion.
# http://rubyforge.org/tracker/index.php?func=detail&aid=16741&group_id=4550&atid=17564
#require 'DBD/Mysql/Mysql'
#class DBI::DBD::Mysql::Database
#  TYPE_MAP.default = TYPE_MAP[nil] if TYPE_MAP.default.nil?
#end

def output_query ( sth, header = false )
  str = header ?  "Time" : "#{Time.now}" 
  while row = sth.fetch_array do
    # output prefix data for sockett
    # output all the processlist data
    str += header ? ",#{row[0]}" : ",#{row[1]}"
  end
  return str
end


$options = {}
$options[:dbuser] = nil
$options[:dbpass] = nil
$options[:dbport] = 3306
$options[:dbsocket] = nil
$options[:daemonize] = false
$options[:pidfile] = Dir.pwd + "/mysample.pid"
$options[:interval] = 10 #seconds
$options[:command] = ProcessCtl::STARTCMD


user  = "root";
pass  = "n0m0r3181";
dsn   = "DBI:Mysql:host=localhost;port=3306"
query = "show global status;"

opts = OptionParser.new
opts.banner = "Usage $0 [OPTIONS]"
opts.on("-u", "--user USER", String, "MySQL User" )  { |v|  $options[:dbuser] = v }
opts.on("-p", "--pass PASSWORD", String, "MySQL Password" )  { |v|  $options[:dbpass] = v }
opts.on("-P", "--pidfile PIDFILE", String, "PID File (default: #{$options[:pidfile]})" )  { |v|  $options[:pidfile] = v }
opts.on("-d", "--daemonize", "daemonize process (default: #{$options[:daemonize]}" )  { |v|  $options[:daemonize] = true }
opts.on("-k", "--command (start|stop|status)", String, "command to pass daemon") {|v| $options[:command] =
  v }
opts.on("-h", "--help",  "this message") { puts opts; exit 1}
opts.parse!

interrupted = false
trap("INT") { interrupted = true }

DBI.connect(dsn, user, pass) do |dbh|
  sth = dbh.execute(query) 
  puts output_query(sth,true) if sth
  loop do
    begin
      sth = dbh.execute(query) 
      puts output_query(sth) if sth
    rescue DBI::DatabaseError => e
      puts "An error occurred"
      puts "Error code: #{e.err}"
      puts "Error message: #{e.errstr}"
      puts "Error SQLSTATE: #{e.state}"
#    rescue Exception => e
#     puts e.inspect
    ensure
      sth.finish if sth
    end
    exit if interrupted
    sleep 10
  end
end

pc = ProcessCtl.new
pc.daemonize = $options[:daemonize]
pc.pidfile   = $options[:pidfile]
exit
case $options[:command]
  when ProcessCtl::STOPCMD
    pc.stop
  when ProcessCtl::STATUSCMD
    exit pc.status
  else
    exit pc.start { mpl.run  }
end
