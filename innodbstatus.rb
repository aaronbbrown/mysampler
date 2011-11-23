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
        case k
        when "BACKGROUND THREAD" then
        when "SEMAPHORES" then
        when "FILE I/O" then
        when "INSERT BUFFER AND ADAPTIVE HASH INDEX" then
        when "BUFFER POOL AND MEMORY" then
        when "ROW OPERATIONS" then
        when "LATEST DETECTED DEADLOCK" then
        when "TRANSACTIONS" then
        end
      end

    end

    def self.parse_background_thread 
    end
  end
end
