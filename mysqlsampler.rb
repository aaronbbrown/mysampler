require 'rubygems'
require 'sequel'

require File.dirname(__FILE__) + '/file.rb'

module MySQLSampler
  CSVOUT, YAMLOUT,GRAPHITEOUT = 0,1,2

  class Connection
    attr_reader :sequel

    def initialize ( h = {} )
      @user   ||= h[:user]
      @pass   ||= h[:pass]
      @socket ||= h[:socket]
      @port   = h[:port] || 3306
      @host   = h[:host] || "localhost"

      params = { :host     => @host, 
                 :user     => @user, 
                 :port     => @port,
                 :password => @pass }
      params[:socket] = @socket if @socket    
      @sequel = Sequel.mysql(params)
    end

    # get the real hostname of the MySQL Server that we are connected to
    def get_mysql_hostname 
      @sequel["SELECT @@hostname;"].first[:@@hostname]
    end

    def mysql_version_exact
      @sequel["SELECT @@version;"].first[:@@version]
    end

    def mysql_version
      ver = mysql_version_exact.split(".")
      ver.values_at(0,1).join(".")
    end

    def engine_supported( engine )
      h = @sequel["SHOW ENGINES"].to_hash(:Engine,:Support)
      h[engine] =~ /(YES|DEFAULT)/
    end
  end

  class GraphiteLogger
  end

  class CSVLogger
  end

  class QueryHash
    def initialize ( h = {} )
      @connection  ||= h[:connection]
      @query       ||= h[:query]
      @keycolumn   ||= h[:keycolumn]
      @valuecolumn ||= h[:valuecolumn]
      @value_regex ||= h[:value_regex]
      @key_prefix  ||= h[:key_prefix]
    end

    def execute
      return {} unless @query
      h = prefix_keys(@connection.sequel[@query].to_hash(@keycolumn,@valuecolumn))
      apply_value_regex(h)
    end

    def apply_value_regex(h)
      return h unless @value_regex
      Hash[h.map { |k,v| v =~ @value_regex ? [k,$1] : [k,v] }]
    end

    def get_header_rows
      prefix_keys(@connection.sequel[@query].to_hash(@keycolumn,@valuecolumn)).keys
    end

    def normalize ( str )
      str.gsub(/[^[:alnum:]\._]/, '_')
    end

    def prefix_keys ( h )
      Hash[h.map { |k,v| [ normalize("#{@key_prefix}#{k}"), v] }]
    end
  end

  class MutexHash < QueryHash
    def initialize ( h = {} )
      h.merge!( :keycolumn   => :Name,
                :valuecolumn => :Status,
                :key_prefix  => "mutex.",
                :value_regex => /os_waits=(\d+)/ )
      h[:query] = which_query h[:connection]

      super h
    end

    def which_query ( connection )
      version = connection.mysql_version
      if version == "5.0"
        "SHOW MUTEX STATUS"
      elsif version.to_f > 5.0
        "SHOW ENGINE INNODB MUTEX"
      else
        # not supported
        nil
      end
    end

    def execute
      super if @connection.engine_supported("InnoDB")
    end
  end

  class MySQLSampler
    attr_accessor :connection, :interval, :output, :relative, :outputfn, :rotateinterval, :graphitehost

    def initialize
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
      @sequel = @connection.sequel
      status  = QueryHash.new( :query       => "SHOW GLOBAL STATUS",       
                               :connection  => @connection, 
                               :keycolumn   => :Variable_name, 
                               :valuecolumn => :Value, 
                               :key_prefix  => "status.")
      mutexes = MutexHash.new( :connection  => @connection )

      if @output == GRAPHITEOUT 
        @mysql_hostname = @connection.get_mysql_hostname 
        conn_to_graphite
      else
# this isn't going to work right - order of header rows might not match the merged hashes
        headers = status.get_header_rows
        headers += mutexes.get_header_rows
        open_rotating_file(:header => "Time," + headers.sort.join(","), :interval => @rotateinterval) 
      end

      loop do
        begin
          rows = status.execute
          rows.merge!(mutexes.execute)
          rows = values_to_numeric(rows)
          rows = calc_relative(rows) if @relative
          output_query(rows) 
        rescue Exception => e
          STDERR.puts "An error occurred #{e}"
        end

        sleep @interval
      end
      @rf.close if @rf && @outputfn
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

  end
end
