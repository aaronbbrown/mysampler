require 'pp'
require 'timecop'
require 'mysqlsampler'
require 'innodbstatus'

describe "database connectivity" do
  before(:all) do
    @host = "locutus.borg.lan"

    @db = MySQLSampler::Connection.new( :user => "aaron",
                                        :pass => "n0m0r3181",
                                        :host => @host,
                                        :port => 3306 )
    @ms = MySQLSampler::MySQLSampler.new
    @ms.connection = @db

    @status  = MySQLSampler::QueryHash.new( :query       => "SHOW GLOBAL STATUS",       
                              :connection  => @db,
                              :keycolumn   => :Variable_name, 
                              :valuecolumn => :Value, 
                              :key_prefix  => "status.")
  end

  it "should return 'locutus.borg.lan'" do
    @db.get_mysql_hostname.should == @host
  end

  it "should return an array that includes 'Uptime'" do
    @status.get_header_rows.should include('status.Uptime')
  end

end

describe "Row operations" do
  before(:all) do
    Timecop.freeze(Time.local(2011,10,17,0,0,0))
    @ms = MySQLSampler::MySQLSampler.new
  end

  after(:all) do
    Timecop.return
  end

  # test relative operation
  it "should return a key with 2 as the value" do
    @ms.calc_relative({"Foo" => 4})
    h = @ms.calc_relative({"Foo" => 6})
    h["Foo"].should == 2
  end

  it "should prefix key with 'foo.'" do
    h = @ms.prefix_keys({"bar" => 1}, "foo.")
    h.keys.first.should == "foo.bar"
  end

  it "should convert a string to a numeric if it is a number" do
    @ms.to_numeric("5").should == 5
  end

  it "should not convert a string to a numeric if it is a string" do
    @ms.to_numeric("a").should == "a"
  end

  it "should convert a hash of string numbers to numbers" do
    h = @ms.values_to_numeric({"a" => "1"})
    h["a"].should == 1
  end

  it "should convert a hash into a comma separated csv prefixed by time" do
    h = { "a" => 1, "b" => 2, "c" => "foo" }
    @ms.hash_to_csv(h).should == "2011-10-17 00:00:00,1,2,foo"
  end

end


describe "INNODB STATUS Parser" do
  before(:all) do
    @is = MySQLSampler::InnoDBStatusHash.new
    @data = IO.read(File.dirname(__FILE__)+"/innodbstatus.txt")
  end
  
  it "should return a hash w/ multiple keys" do
    MySQLSampler::InnoDBStatusParser.sections(@data).keys.should include('TRANSACTIONS')
  end
end
