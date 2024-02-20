STORAGE_FILE = "../.data"

class Storage
  def initialize()
    # https://crystal-lang.org/api/1.11.2/File.html#constructor-detail
    @file = File.open STORAGE_FILE, mode: "a+"
  end

  def write(s : String)
    # File.write STORAGE_FILE, s + "\n", mode: "a"
    @file.print s + "\n" # appends because of mode
    @file.flush # maybe move to SIGINT trap
  end

  def read()
    File.read STORAGE_FILE
  end

  def close()
    @file.close
  end
end
