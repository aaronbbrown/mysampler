class MySQLSampler
  attr_accessor :user, :pass, :port, :socket, :host, :interval

  def initialize
    @user = nil
    @pass = nil
    @port = 3306
    @socket = nil
    @host = "localhost"
    @query = "SHOW GLOBAL STATUS"
    @interval = 10
  end

  def run
    DBI.connect(dsn, @user, @pass) do |dbh|
      sth = dbh.execute(@query) 
      puts output_query(sth,true) if sth
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
  def output_query ( sth, header = false )
    str = header ?  "Time" : "#{Time.now}" 
    while row = sth.fetch_array do
      # output prefix data for sockett
      # output all the processlist data
      str += header ? ",#{row[0]}" : ",#{row[1]}"
    end
    return str
  end

  def dsn
    result = "DBI:Mysql:host=#{@host};port=#{@port}"
    result += ";socket=#{@socket}" if @socket
    return result
  end
end


