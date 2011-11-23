module MySQLSampler
  class InnoDBStatusHash < QueryHash
    def initialize ( h = {} )
      h.merge!( :key_prefix  => "innodbstatus.",
                :query       => "SHOW ENGINE INNODB STATUS\G" )

      super h
    end

    def execute
      return nil unless @connection.engine_supported("InnoDB")
      ds = @connection.sequel[@query]
      h = InnoDBStatusParser.parse(ds.to_a[0][:Status])
      prefix_keys(h) 
    end
  end

  module InnoDBStatusParser
    def self.sections ( status )
      # find :
      # -----
      # WORDS IN CAPITAL LETTERS
      # -----
      # body
      Hash[status.scan /-+\n([[:upper:][:space:][:punct:]]+)\n-+\n(.*?)(?=\n-+\n[[:upper:][:space:][:punct:]]+\n)/m]
    end

    def self.parse ( status )
      h = {}
      sections(status).map do |k,v|
        h.merge! case k
        when "BACKGROUND THREAD" then
          parse_background_thread v
        when "SEMAPHORES" then
          parse_semaphores v
        when "FILE I/O" then
          parse_fileio v
        when "INSERT BUFFER AND ADAPTIVE HASH INDEX" then
          parse_insert_buffer v
        when "BUFFER POOL AND MEMORY" then
          parse_buffer_pool v
        when /^ROW/ then
          parse_row_ops v
        when "LATEST DETECTED DEADLOCK" then
          parse_deadlocks v
        when "TRANSACTIONS" then
          parse_transactions v
        else
          {}
        end
      end
   end

    def self.parse_background_thread ( data )
      #  srv_master_thread loops: 26645693 1_second, 26645613 sleeps, 266456 10_second, 455 background, 455 flush
      #  srv_master_thread log flush and writes: 2937913
      regexes = { /(\d+) 1_second,/   => "srv_master_thread_loops.1_second",
                  /(\d+) sleeps,/     => "srv_master_thread_loops.sleeps",
                  /(\d+) 10_second,/  => "srv_master_thread_loops.10_second",
                  /(\d+) background,/ => "srv_master_thread_loops.background",
                  /(\d+) flush,/      => "srv_master_thread_loops.background",
                  /flush and writes: (\d+)/ => "srv_master_thread.log_flush_and_writes" }
      self.generic_parse(regexes, data)
    end

    def self.parse_semaphores ( data )
      # OS WAIT ARRAY INFO: reservation count 15790436, signal count 18650446
      # Mutex spin waits 6231859458, rounds 9218278203, OS waits 7750045
      # RW-shared spins 7207433, OS waits 656673; RW-excl spins 97132, OS waits 408931
      # Spin rounds per wait: 1.48 mutex, 10.35 RW-shared, 177.41 RW-excl
      regexes = { /reservation count (\d+)/     => "os_wait.reservations",
                  /signal count (\d+)/          => "os_wait.signals",
                  /^Mutex.*spin waits (\d+)/    => "mutex.spin_waits",
                  /^Mutex.*rounds (\d+)/        => "mutex.rounds",
                  /^Mutex.*OS waits (\d+)/      => "mutex.os_waits",
                  /RW-shared.*spins (\d+)/     => "rw_shared.spins",
                  /RW-shared.*OS waits (\d+)/  => "rw_shared.os_waits",
                  /RW-excl.*spins (\d+)/       => "rw_excl.spins",
                  /RW-excl.*OS waits (\d+)/    => "rw_excl.os_waits",
                  /^Spin rounds per wait:.*(\d+) mutex/     => "mutex.rounds_per_wait",
                  /^Spin rounds per wait:.*(\d+) RW-shared/ => "rw_shared.rounds_per_wait",
                  /^Spin rounds per wait:.*(\d+) RW-excl/   => "rw_excl.rounds_per_wait" }
      self.generic_parse(regexes,data)
    end
    
    def self.parse_fileio ( data )
      {}
    end

    def self.parse_insert_buffer ( data )
      {}
    end

    def self.parse_buffer_pool ( data )
      {}
    end

    def self.parse_row_ops ( data )
      {}
    end

    def self.parse_deadlocks ( data )
      {}
    end

    def self.parse_transactions ( data )
      {}
    end

    def self.generic_parse ( regexes, data )
      h = {}
      regexes.each do |k,v|
        if data =~ k 
          h[v] = $1
        end
      end
      h
    end
 
  end
end
