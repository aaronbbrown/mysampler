#!/usr/bin/ruby

require 'optparse'
require 'processctl.rb'
require 'mysqlsampler.rb'
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

$options = {}
$options[:dbuser] = nil
$options[:dbpass] = nil
$options[:dbhost] = "localhost"
$options[:dbport] = 3306
$options[:dbsocket] = nil
$options[:daemonize] = false
$options[:pidfile] = Dir.pwd + "/mysample.pid"
$options[:interval] = 10 #seconds
$options[:command] = ProcessCtl::STARTCMD
$options[:format] = "YAML"


opts = OptionParser.new
opts.banner = "Usage $0 [OPTIONS]"
opts.on("-u", "--user USER", String, "MySQL User" )  { |v|  $options[:dbuser] = v }
opts.on("-p", "--pass PASSWORD", String, "MySQL Password" )  { |v|  $options[:dbpass] = v }
opts.on("-P", "--port PORT", Integer, "MySQL port (default #{$options[:dbport]})" )  { |v|  $options[:dbport] = v }
opts.on("--pidfile PIDFILE", String, "PID File (default: #{$options[:pidfile]})" )  { |v|  $options[:pidfile] = v }
opts.on("-H", "--host HOST", String, "MySQL hostname (default: #{$options[:dbhost]})" )  { |v|  $options[:dbhost] = v }
opts.on("-i", "--interval SECONDS", Integer, "Interval between runs (default: #{$options[:interval]})" )  { |v|  $options[:interval] = v }
opts.on("-d", "--daemonize", "daemonize process (default: #{$options[:daemonize]})" )  { |v|  $options[:daemonize] = true }
opts.on("-k", "--command (start|stop|status)", String, "command to pass daemon") do |v|
  $options[:command] = case v
    when "stop"
      ProcessCtl::STOPCMD
    when "status"
      ProcessCtl::STATUSCMD
    when "start"
      ProcessCtl::STARTCMD
    else
      puts opts
      exit 1
  end
end
opts.on("-h", "--help",  "this message") { puts opts; exit 1}
opts.parse!

pc = ProcessCtl.new
pc.daemonize = $options[:daemonize]
pc.pidfile   = $options[:pidfile]


ms = MySQLSampler.new
ms.user = $options[:dbuser]
ms.pass = $options[:dbpass]
ms.host = $options[:dbhost]
ms.port = $options[:dbport]
ms.socket = $options[:dbsocket]
ms.interval = $options[:interval]


case $options[:command]
  when ProcessCtl::STOPCMD
    pc.stop { puts "I'm done" }
  when ProcessCtl::STATUSCMD
    exit pc.status
  else
    exit pc.start { ms.run }
end
