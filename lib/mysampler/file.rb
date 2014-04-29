#!/usr/bin/ruby

require 'yaml'
module MySampler

class FileRotating
  YEAR, MONTH, DAY, HOUR, MINUTE, SECOND = 0,1,2,3,4,5

  attr_accessor :header

  def initialize (params, *args)
    @interval = params[:interval] || DAY
    @header   = params[:header]   || nil  # a header to put at the top of every file
    @args = args
    @root_fn = args[0]
    @stamp = get_date_stamp

    open_local do |f|
      f.puts @header if @header
      if block_given? 
        return yield f
      else 
        return f
      end
    end
  end

  def close 
    if @f
      @f.flock(File::LOCK_UN)
      @f.close 
    end
  end

private
  def open_local
    fn = sprintf("%s.%s",@root_fn,@stamp)
    args = @args
    args[0] = fn
#    puts fn

    begin
      @f = File.open(*args) 
      @f.flock(File::LOCK_EX) if @f
      if block_given?
        return yield self
      else
        return self
      end
    end
  end


  def get_date_stamp
    format = case @interval
      when YEAR   then '%Y'
      when MONTH  then '%Y%m'
      when DAY    then '%Y%m%d'
      when HOUR   then '%Y%m%d%H'
      when MINUTE then '%Y%m%d%H%M'
      when SECOND then '%Y%m%d%H%M%S'
      else raise "Invalid interval"
    end
    return Time.now.strftime(format)
  end

  def method_missing(method, *args, &block)
    # check to see if we need to reopen
    stamp = get_date_stamp

    if @stamp != stamp
      @stamp = stamp
      close 
      open_local 
      @f.puts @header if @header
    end
    return @f.send(method, *args, &block)       
  end

end

#FileRotating.new({:interval => FileRotating::SECOND, :header => "Header!!!"}, "/tmp/foo.txt", "w") do |f|
#  5.times do
#    f.puts Time.now
#    f.puts "Foo!"
#    sleep 1
#  end
#end

end
