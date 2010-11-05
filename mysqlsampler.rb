require 'csv'
require File.dirname(__FILE__) + '/file.rb'

class MySQLSampler
  CSVOUT, YAMLOUT = 0,1
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
  end

  def run
    DBI.connect(dsn, @user, @pass) do |dbh|
      sth = dbh.execute(@query) 
      rows = rows_to_xhash(sth)
      params = { :interval => @rotateinterval }
      params[:header] =  hash_to_csv(rows,true) if sth 

      f = @outputfn ? FileRotating.new(params, @outputfn, "w") : STDOUT
      puts(header) unless f && @outputfn

      loop do
        begin
          sth = dbh.execute(@query) 
          f.puts output_query(sth) if sth
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
      f.close if f && @outputfn
    end
  end

protected
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

  def output_query ( sth )
    result = ""

    raw_rows = rows_to_xhash(sth) 
    rows = @relative ? calc_relative(raw_rows) : raw_rows
    @prev_rows = raw_rows
    case @output
      when YAMLOUT then
#        result = YAML::dump({time => Time.now, rows})
      else # CSVOUT 
        result = hash_to_csv(rows)
    end
    return result
  end

  # the query comes back in 2 columns.  Convert the rows to crosstab xhash entries
  # takes an open statement handle
  def rows_to_xhash( sth )
    result = {}
    while row = sth.fetch_array do
      # does it look like a number?
     result[row[0]] = (row[1].is_a?(String) && (row[1] == row[1].to_i.to_s)) ? row[1].to_i : row[1]
    end
    return result
  end

  def hash_to_csv ( rows, header = false )
    csv_str = ""
    str = header ?  "Time" : "#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}" 
    rows.sort.each do |v|
      str += header ? ",#{v[0]}" : ",#{v[1]}"
    end
    return str
#    row = []
#    csv_str = CSV.generate_line do |csv|
#      rows.each do |k,v|
#        row << v
#      end
#      csv << row
#    end
#    return str + "," + csv_str
  end

  def dsn
    result = "DBI:Mysql:host=#{@host};port=#{@port}"
    result += ";socket=#{@socket}" if @socket
    return result
  end
end


