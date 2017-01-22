require 'json'
require 'typhoeus'

API_URL = 'https://azor.weps.cz/api/files/#FILE_ID/download'.freeze

def download_file
  file = File.open(filename, 'wb')
  url = API_URL.sub('#FILE_ID', CONFIG['file'])
  request = Typhoeus::Request.new(
    url,
    userpwd: "#{CONFIG['username']}:#{CONFIG['password']}"
  )
  request.on_headers do |response|
    raise 'Request failed' if response.code != 200
  end
  request.on_body do |chunk|
    file.write(chunk)
  end
  request.on_complete do
    file.close
  end
  request.run
end

def fetch_files
  tries = 5
  begin
    download_file
  rescue => exception
    unless exception.to_s == 'Request failed'
      STDERR.puts "#{exception.class}: #{exception.message}"
      STDERR.puts exception.backtrace
      Kernel.exit(-1)
    end
    if tries.zero?
      Kernel.abort('Downloading file failed! Check API URL and credentials.')
    end
    tries -= 1
    retry
  end
end

def filename
  "#{ENV['KBC_DATADIR']}out/tables/out.c-azor_#{CONFIG['file']}.csv"
end

begin
  CONFIG = JSON.parse(File.read("#{ENV['KBC_DATADIR']}config.json"))['parameters']
rescue StandardError
  Kernel.abort('No configuration file, or it is missing API parameters.')
end
fetch_files
