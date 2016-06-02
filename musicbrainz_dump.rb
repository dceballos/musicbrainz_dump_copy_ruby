require './init.rb'
class MusicbrainzDump
  attr_accessor :base_dir

  def initialize(base_dir)
    raise "Provide base path file" if base_dir.nil?
    @base_dir = base_dir
    ActiveRecord::Base.establish_connection(YAML.load(File.read("database.yml")))
  end

  def restore
    data_files.each do |filename|
      next if filename == '.' || filename == '..'
      path = File.expand_path(File.join(base_dir,filename))
      puts("Truncating #{filename}")
      ActiveRecord::Base.transaction do
        truncate_table(filename)
        file = File.open(path, 'r')
        puts("Copying #{filename}")
        i = 0
        raw_connection.exec("COPY #{filename} FROM STDIN WITH DELIMITER '\t'")
        while !file.eof?
          if (i%1000 == 0)
            puts("Copied #{i} records in #{filename}")
          end
          #copy(filename,path)
          raw_connection.put_copy_data(file.readline)
          i+=1
        end
        raw_connection.put_copy_end
      end
    end
  end

  def raw_connection
    ActiveRecord::Base.connection.raw_connection
  end

  def data_files
    Dir.entries(File.expand_path(@base_dir))
  end

  def truncate_table(table_name)
    raw_connection.exec(%{ 
      TRUNCATE musicbrainz.#{table_name}
    })
  end

  def copy(table_name,file_path)
    ActiveRecord::Base.connection.execute(%{ 
      COPY musicbrainz.#{table_name} 
      FROM '#{file_path}';
    })
  end
end

mbdump = MusicbrainzDump.new(ARGV[0])
mbdump.restore
