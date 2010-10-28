require 'csv'
class MySQLSampler
  CSVOUT, YAMLOUT = 0,1
  attr_accessor :user, :pass, :port, :socket, :host, :interval, :output

  def initialize
    @user = nil
    @pass = nil
    @port = 3306
    @socket = nil
    @host = "localhost"
    @query = "SHOW GLOBAL STATUS"
    @interval = 10
    @output = CSVOUT
  end

  def run
    DBI.connect(dsn, @user, @pass) do |dbh|
      if @output == CSVOUT
        sth = dbh.execute(@query) 
        puts hash_to_csv(sth,true) if sth
      end
      loop do
        begin
          sth = dbh.execute(@query) 
          puts output_query(sth) if sth
        rescue DBI::DatabaseError => e
# this should go to STDERR 
          puts "An error occurred"
          puts "Error code: #{e.err}"
          puts "Error message: #{e.errstr}"
          puts "Error SQLSTATE: #{e.state}"
#          rescue Exception => e
#            puts e.inspect
        ensure
          sth.finish if sth
        end

        sleep @interval
      end
    end
  end

protected
  def output_query ( sth )
    case @output
      when YAMLOUT
      when CSVOUT
        hash_to_csv(sth)
      else
        hash_to_csv(sth)
    end
  end

  # the query comes back in 2 columns.  Convert the rows to crosstab xhash entries
  # takes an open statement handle
  def rows_to_xhash( sth )
    result = {}
    while row = sth.fetch_array do
      result[row[0]] = row[1]
    end
    return result
  end

  def hash_to_csv ( sth, header = false )
    csv_str = ""
    str = header ?  "Time" : "#{Time.now}" 
    rows = rows_to_xhash(sth) 
    row = []
    csv_str = CSV.generate_line do |csv|
      rows.each do |k,v|
        row << v
      end
      csv << row
    end
#    rows.sort do |v|
#      str += header ? ",#{v[0]}" : ",#{v[1]}"
#    end
    return str + "," + csv_str
  end

  def dsn
    result = "DBI:Mysql:host=#{@host};port=#{@port}"
    result += ";socket=#{@socket}" if @socket
    return result
  end
end


