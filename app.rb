require 'bundler/setup'

require 'thumper'

TH = Thumper::Base.new(publish_to: 'amqp://localhost/avnsp',
                       consume_from: 'amqp://localhost/avnsp')

require 'sequel'
DB = Sequel.connect 'postgres://localhost/avnsp'

TH.with_channel prefetch: 100 do |ch|
  ch.subscribe 'event.photo.create', 'photo.uploaded' do |data|
    evt = { name: 'photo', data: data.to_json }
    id = DB[:events].insert(evt)
    ch.publish 'event.photo.created', evt.merge(id: id)
  end
  ch.subscribe 'event.member.create', 'member.created' do |data|
    evt = { name: 'member', data: data.to_json }
    id = DB[:events].insert(evt)
    ch.publish 'event.member.created', evt.merge(id: id)
  end
  ch.subscribe 'event.party.create', 'party.created' do |data|
    evt = { name: 'party', data: data.to_json }
    id = DB[:events].insert(evt)
    ch.publish 'event.party.created', evt.merge(id: id)
  end
end
puts "[INFO] AVNSP event instert handler starting..."
sleep
