require 'csv'
require 'rubygems'
require 'sequel'

require File.dirname(__FILE__) + '/file.rb'

class MySQLSampler
  CSVOUT, YAMLOUT,GRAPHITEOUT = 0,1,2
  attr_accessor :user, :pass, :port, :socket, :host, :interval, :output, :relative, :outputfn, :rotateinterval

  def initialize
    @user = nil
    @pass = nil
    @port = 3306
    @socket = nil
    @host = "localhost"
    @query = "SHOW GLOBAL STATUS"
    @interval = 10
    @relative = false
    @output = CSVOUT
    @prev_rows = {}
    @outputfn = nil
    @rotateinterval = FileRotating::HOUR
    @rf = nil
    @graphite = nil

    conn_to_graphite if @output == GRAPHITEOUT
  end

  def run
    @sequel = db_connect
    get_mysql_hostname 
    if @output == GRAPHITEOUT 
      rows = get_header_rows(dbh)
      open_rotating_file(:header => hash_to_csv(rows,true), :interval => @rotateinterval) 
      output_header(rows) 
    end

    loop do
      begin
        sth = dbh.execute(@query) 
        if sth
          rows = build_hash(sth) 
          output_query(rows) 
        end
      rescue DBI::DatabaseError => e
# this should go to STDERR 
        puts "An error occurred"
        puts "Error code: #{e.err}"
        puts "Error message: #{e.errstr}"
        puts "Error SQLSTATE: #{e.state}"
      ensure
        sth.finish if sth
      end

      sleep @interval
    end
    @rf.close if @rf && @outputfn
  end

protected
  # get the real hostname of the MySQL Server that we are connected to
  def get_mysql_hostname 
    @sequel.run("SELECT @@hostname;")
  end

  def get_header_rows(dbh)
    sth = dbh.execute(@query) 
    rows_to_xhash(sth)
  end

  def output_header(rows)
    @rf.puts(header) if @rf && @outputfn
  end

  def conn_to_graphite
    @graphite = Graphite::Logger.new(@graphitehost)
  end

  def open_rotating_file (params)
    @rf = @outputfn ? FileRotating.new(params, @outputfn, "w") : STDOUT
  end

  def calc_relative(rows)
    result = {}
    rows.each do |k,v|
      if @prev_rows[k] && v.is_a?(Numeric)
        result[k] = v - @prev_rows[k]
      else 
        result[k] = v
      end
    end
    return result
  end

  def build_hash (sth)
    raw_rows = rows_to_xhash(sth) 
    rows = @relative ? calc_relative(raw_rows) : raw_rows
    @prev_rows = raw_rows
    rows
  end

  def output_query (rows )
    case @output
    when YAMLOUT then
#      result = YAML::dump({time => Time.now, rows})
    when GRAPHITEOUT then
      @graphite.log(Time.now.to_i, rows) if @graphite
    else # CSVOUT 
      @rf.puts(hash_to_csv(rows)) if @rf && @outputfn
    end
    true
  end

  def is_a_string? (value)
    value.is_a?(String) && (value == value.to_i.to_s)
  end

  # the query comes back in 2 columns.  Convert the rows to crosstab xhash entries
  # takes an open statement handle
  def rows_to_xhash( sth )
    result = {}
    while row = sth.fetch_array do
      k = @ouput == GRAPHITEOUT ? "mysql.globalstatus.#{@mysql_hostname}.#{row[0]}" : row[0]

      if @output == GRAPHITEOUT
        #skip anything that isn't a number
        next if is_a_string?(row[1])
      end
   
      # convert number-like strings to integers
      result[k] = is_a_string?(row[1]) ? row[1].to_i : row[1]
    end
    return result
  end

  def hash_to_csv ( rows, header = false )
    csv_str = ""
    str = header ?  "Time" : "#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}" 
    rows.sort.each { str += header ? ",#{v[0]}" : ",#{v[1]}" } 
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

  def dsn
    result = "DBI:Mysql:host=#{@host};port=#{@port}"
    result += ";socket=#{@socket}" if @socket
    return result
  end
end


