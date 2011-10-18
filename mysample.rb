#!/usr/bin/env ruby

require 'optparse'
require File.dirname(__FILE__) + '/processctl.rb'
require File.dirname(__FILE__) + '/mysqlsampler.rb'
require File.dirname(__FILE__) + '/file.rb'
require 'rubygems'
require 'sequel'
require 'logger'

$options = { :dbuser    => nil,
             :dbpass    => nil,
             :dbhost    => "localhost",
             :dbport    => 3306,
             :dbsocket  => nil,
             :daemonize => false,
             :pidfile   => Dir.pwd + "/mysample.pid",
             :interval  => 10, #seconds
             :command   => ProcessCtl::STARTCMD ,
             :output    => MySQLSampler::CSVOUT,
             :relative  => true,
             :graphitehost => nil, }

opts = OptionParser.new
opts.banner = "Usage #{$0} [OPTIONS]"
opts.on("-u", "--user USER",     String,  "MySQL User" )  { |v|  $options[:dbuser] = v }
opts.on("-p", "--pass PASSWORD", String,  "MySQL Password" )  { |v|  $options[:dbpass] = v }
opts.on("-P", "--port PORT",     Integer, "MySQL port (default #{$options[:dbport]})" )  { |v|  $options[:dbport] = v }
opts.on("--pidfile PIDFILE",     String,  "PID File (default: #{$options[:pidfile]})" )  { |v|  $options[:pidfile] = v }
opts.on("-H", "--host HOST",     String,  "MySQL hostname (default: #{$options[:dbhost]})" )  { |v|  $options[:dbhost] = v }
opts.on("-f", "--file FILENAME", String,  "output filename (will be appended with rotation timestamp)" )  { |v|  $options[:outputfn] = v }
opts.on("-o", "--output (csv|graphite)", String, "Output format (default: csv)" ) do |v| 
  $options[:output] = case v
    when "yaml"
      MySQLSampler::YAMLOUT
    when "csv"
      MySQLSampler::CSVOUT
    when "graphite"
      require 'graphite/logger'
      MySQLSampler::GRAPHITEOUT
    else
      puts opts
      exit 1
  end
end
opts.on("-i", "--sleep SECONDS", Integer, "Interval between runs (default: #{$options[:interval]})" )  { |v|  $options[:interval] = v }
opts.on("-r", "--relative","Show the difference between the current and previous values (default: #{$options[:relative]})" )  { |v|  $options[:relative] = v }
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
opts.on("-g", "--graphite HOST:PORT", String, "Graphite server:port") { |v| $options[:graphitehost] = v }
opts.on("-h", "--help",  "this message") { puts opts; exit 1}
opts.parse!

pc = ProcessCtl.new
pc.daemonize = $options[:daemonize]
pc.pidfile   = $options[:pidfile]


ms = MySQLSampler.new
ms.user     = $options[:dbuser]
ms.pass     = $options[:dbpass]
ms.host     = $options[:dbhost]
ms.port     = $options[:dbport]
ms.socket   = $options[:dbsocket]
ms.interval = $options[:interval]
ms.output   = $options[:output]
ms.relative = $options[:relative]
ms.outputfn = $options[:outputfn] if $options[:outputfn]
ms.rotateinterval = FileRotating::HOUR
ms.graphitehost = $options[:graphitehost] if ms.output == MySQLSampler::GRAPHITEOUT

case $options[:command]
when ProcessCtl::STOPCMD
  pc.stop { puts "I'm done" }
when ProcessCtl::STATUSCMD
  exit pc.status
else
  exit pc.start { ms.run }
end
