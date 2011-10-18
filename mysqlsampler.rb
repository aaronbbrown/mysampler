require 'rubygems'
require 'sequel'

require File.dirname(__FILE__) + '/file.rb'

class MySQLSampler
  CSVOUT, YAMLOUT,GRAPHITEOUT = 0,1,2
  attr_accessor :user, :pass, :port, :socket, :host, :interval, :output, :relative, :outputfn, :rotateinterval, :graphitehost

  def initialize
    @user           = nil
    @pass           = nil
    @port           = 3306
    @socket         = nil
    @host           = "localhost"
    @query          = "SHOW GLOBAL STATUS"
    @interval       = 10
    @relative       = false
    @output         = CSVOUT
    @prev_rows      = {}
    @outputfn       = nil
    @rotateinterval = FileRotating::HOUR
    @rf             = nil
    @graphitehost   = nil
    @graphite       = nil
    @mysql_hostname = nil
  end

  def run
    @sequel = db_connect
    if @output == GRAPHITEOUT 
      get_mysql_hostname 
      conn_to_graphite if @output == GRAPHITEOUT
    else
      headers = get_header_rows
      open_rotating_file(:header => headers.join(","), :interval => @rotateinterval) 
    end

    loop do
      begin
        rows = @sequel[@query].to_hash(:Variable_name,:Value)
        rows = values_to_numeric(rows)
        rows = calc_relative(rows)
        output_query(rows) 
      rescue Exception => e
        STDERR.puts "An error occurred #{e}"
      end

      sleep @interval
    end
    @rf.close if @rf && @outputfn
  end

  # get the real hostname of the MySQL Server that we are connected to
  def get_mysql_hostname 
    @mysql_hostname = @sequel["SELECT @@hostname;"].first[:@@hostname]
  end

  def get_header_rows
    @sequel[@query].to_hash(:Variable_name,:Value).keys
  end

  def output_header(rows)
    @rf.puts(header) if @rf && @outputfn
  end

  def conn_to_graphite
    @graphite = Graphite::Logger.new(@graphitehost)
#    @graphite.logger = Logger.new('graphite.out')
  end

  def open_rotating_file (params)
    @rf = @outputfn ? FileRotating.new(params, @outputfn, "w") : STDOUT
  end

  def calc_relative(rows)
    result = {}
    rows.each do |k,v|
      if @prev_rows[k] && numeric?(v)
        result[k] = v - @prev_rows[k]
      else 
        result[k] = v
      end
    end
    @prev_rows = rows
    return result
  end

  def prefix_keys ( h, prefix )
    Hash[h.map { |k,v| [ "#{prefix}#{k}", v] }]
  end

  def numeric? (value)
    true if Float(value) rescue false
  end

  def to_numeric (value)
    numeric?(value) ? value.to_i : value
  end
  
  def values_to_numeric ( h )
    Hash[h.map { |k,v| [ k, to_numeric(v)] }]
  end

  def output_query (rows )
    case @output
    when YAMLOUT then
#      result = YAML::dump({time => Time.now, rows})
    when GRAPHITEOUT then
      graphite_rows = prefix_keys(rows, "mysql.#{@mysql_hostname.split('.').reverse.join('.')}.")
      @graphite.log(Time.now.to_i, graphite_rows) if @graphite
    else # CSVOUT 
      @rf.puts(hash_to_csv(rows)) if @rf && @outputfn
    end
    true
  end

  def hash_to_csv ( rows, header = false )
    str = header ?  "Time" : "#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}" 
    rows.sort.each { |v| str += header ? ",#{v[0]}" : ",#{v[1]}" } 
    return str
  end

  def db_connect
    params = { :host => @host, 
               :user => @user, 
               :port => @port,
               :password => @pass }
    params[:socket] = @socket if @socket    
    @sequel = Sequel.mysql(params)
  end
end


