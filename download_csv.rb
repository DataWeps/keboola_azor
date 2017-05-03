require 'json'
require 'typhoeus'

API_FILE_LIST_URL = 'https://azor.weps.cz/api/files'.freeze
API_URL = 'https://azor.weps.cz/api/files/#FILE_ID/download'.freeze
MANIFEST = {
  incremental: true,
  primary_key: %w(internal_code shop saved_at)
}.freeze
ALLOWED_FILE_REGEXP = /autoexport$|portfolio$/


def download_file(token)
  file = File.open(filename(token), 'wb')
  url = API_URL.sub('#FILE_ID', token)
  request = Typhoeus::Request.new(
    url,
    userpwd: "#{CONFIG['username']}:#{password}"
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
    tokens = fetch_file_tokens
    tokens.each { |token| download_file(token) }
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

def fetch_file_tokens
  response = Typhoeus::Request.new(
    API_FILE_LIST_URL,
    userpwd: "#{CONFIG['username']}:#{password}"
  ).run
  json = JSON.parse(response.body)
  client = json['client']
  tokens = json['files'].collect { |file| file['token'] }
  tokens.keep_if do |token|
    token.start_with?(client) && token =~ ALLOWED_FILE_REGEXP
  end
  tokens
end

def filename(token)
  "#{ENV['KBC_DATADIR']}out/tables/out.c-azor.#{token}.csv"
end

def create_manifest
  Dir["#{ENV['KBC_DATADIR']}out/tables/*autoexport.csv"].each do |table|
    File.open("#{table}.manifest", 'w') { |file| file << MANIFEST.to_json }
  end
end

def password
  CONFIG['#password'] ? CONFIG['#password'] : CONFIG['password']
end

begin
  CONFIG = JSON.parse(File.read("#{ENV['KBC_DATADIR']}config.json"))['parameters']
rescue StandardError
  Kernel.abort('No configuration file, or it is missing API parameters.')
end
fetch_files
create_manifest
