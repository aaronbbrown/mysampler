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
    @query          = 'SHOW /*!50002 GLOBAL */ STATUS'
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

    first_run = true
    loop do
      begin
        rows = @sequel[@query].to_hash(:Variable_name,:Value)
        rows = values_to_numeric(rows)
        rows = calc_relative(rows) if @relative
        output_query(rows) unless first_run && @relative
        first_run = false
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

  def is_counter? (key)
    # list lovingly stolen from pt-mysql-summary
    !%w[ Compression Delayed_insert_threads Innodb_buffer_pool_pages_data 
         Innodb_buffer_pool_pages_dirty Innodb_buffer_pool_pages_free 
         Innodb_buffer_pool_pages_latched Innodb_buffer_pool_pages_misc 
         Innodb_buffer_pool_pages_total Innodb_data_pending_fsyncs 
         Innodb_data_pending_reads Innodb_data_pending_writes 
         Innodb_os_log_pending_fsyncs Innodb_os_log_pending_writes 
         Innodb_page_size Innodb_row_lock_current_waits Innodb_row_lock_time_avg 
         Innodb_row_lock_time_max Key_blocks_not_flushed Key_blocks_unused 
         Key_blocks_used Last_query_cost Max_used_connections Ndb_cluster_node_id 
         Ndb_config_from_host Ndb_config_from_port Ndb_number_of_data_nodes 
         Not_flushed_delayed_rows Open_files Open_streams Open_tables 
         Prepared_stmt_count Qcache_free_blocks Qcache_free_memory 
         Qcache_queries_in_cache Qcache_total_blocks Rpl_status 
         Slave_open_temp_tables Slave_running Ssl_cipher Ssl_cipher_list 
         Ssl_ctx_verify_depth Ssl_ctx_verify_mode Ssl_default_timeout 
         Ssl_session_cache_mode Ssl_session_cache_size Ssl_verify_depth 
         Ssl_verify_mode Ssl_version Tc_log_max_pages_used Tc_log_page_size 
         Threads_cached Threads_connected Threads_running 
         Uptime_since_flush_status ].include? key
  end

  def calc_relative(rows)
    result = {}
    rows.each do |k,v|
      if @prev_rows[k] && numeric?(v) && is_counter?(k)
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
    numeric?(value) ? scale_value(value.to_i) : value
  end

  # scale the value to be per @interval if recording relative values
  # since it doesn't make much sense to output values that are "per 5 seconds"
  def scale_value (value)
    @relative ? (value/@interval) : value
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


