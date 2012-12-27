require 'wombat'
require 'pp'
require 'pg'
require 'uri'

p = URI.parse(ENV['DATABASE_URL'] || 'postgres://localhost/fantasy')

if p.user
  db = PGconn.open(:host => p.host, :port => p.port, :user => p.user, :password => p.password, :dbname => p.path[1..-1])
else
  db = PGconn.open(:host => p.host, :port => p.port, :dbname => p.path[1..-1])
end

#############################
# Create Current Roster Table
#############################
db.exec "CREATE TABLE IF NOT EXISTS roster (player_name TEXT, slot TEXT, fteam INT)"
db.exec "DELETE FROM roster *"

(1..8).each do |team_id|
  data = Wombat.crawl do
    base_url "http://games.espn.go.com"
    path "/fba/clubhouse?leagueId=202659&teamId=#{team_id}"

    players "css=.pncPlayerRow", :iterator do
      name  "css=td.playertablePlayerName a"
      slot  "css=td.playerSlot"
    end
  end
  data["players"].each do |p|
    if p["name"] == nil
      next
    end
    p["name"].gsub!(/[`'"]|(\ \ )/," ")
    query = "INSERT INTO roster VALUES ('#{p["name"]}', '#{p["slot"]}', #{team_id});"
    pp query
    db.exec query
  end
end

############################
# Insert Team Player Records
############################
db.exec "CREATE TABLE IF NOT EXISTS fantasy (player_name TEXT, team TEXT, fteam INT, min INT, fgm INT, fga INT, ftm INT, fta INT, reb INT, ast INT, stl INT, blk INT, tover INT, pts INT, fpts INT, opp TEXT, slot TEXT, period_id INT)"

opening_night = Time.parse("30/10/2012")
one_day = 60*60*24
current_period = ((Time.now - opening_night) / one_day).to_i

last_update = ((db.exec "SELECT MAX(period_id) FROM fantasy;")[0][0] or 0) + 1

(last_update..current_period).each do |period_id|
  ###########################
  # Insert players on teams
  ###########################
  (1..8).each do |fteam|
    data = Wombat.crawl do
      base_url "http://games.espn.go.com"
      path "/fba/clubhouse?leagueId=202659&teamId=#{fteam}&seasonId=2013&scoringPeriodId=#{period_id}"

      players "css=.pncPlayerRow", :iterator do
        name  "css=td.playertablePlayerName"
        opp   "css=td div a"
        slot  "css=td.playerSlot"
        stats "css=td.playertableStat", :list
      end
    end
    data["players"].each do |p|
      stats = p["stats"]
      next if stats[0] == '--'
      fullName = p["name"].split(/,\s*/)
      name = fullName[0].gsub(/[`'"]|(\s+)/," ").gsub(/\*/, "")
      team = fullName[1][0,3]
      query = "INSERT INTO fantasy VALUES ('#{name}', '#{team}',  #{fteam}, '', #{stats[0]}, #{stats[1]}, #{stats[2]}, #{stats[3]}, #{stats[4]}, #{stats[5]}, #{stats[6]}, #{stats[7]}, #{stats[8]}, #{stats[9]}, #{stats[10]}, '#{p["opp"]}', '#{p["slot"]}', #{period_id});"
      pp query
      db.exec query
    end
  end

  ###########################
  # Insert free agent records
  ###########################
  (0..250).step(50).each do |start_idx|
    data = Wombat.crawl do
      base_url "http://games.espn.go.com"
      path "/fba/leaders?leagueId=202659&scoringPeriodId=#{period_id}&seasonId=2013&startIndex=#{start_idx}"

      players "css=.pncPlayerRow", :iterator do
        name  "css=td.playertablePlayerName"
        opp   "css=td div a"
        stats "css=td.playertableStat", :list
      end
    end
    data["players"].each do |p|
      stats = p["stats"]
      break if stats[0] == '--'
      fullName = p["name"].split(/,\s*/)
      name = fullName[0].gsub(/[`'"]|(\s+)/," ").gsub(/\*/, "")
      team = fullName[1][0,3]
      if db.exec("SELECT count(*) FROM fantasy WHERE player_name = '#{name}' AND period_id = #{period_id};")[0][0] == 0
        query = "INSERT INTO fantasy VALUES ('#{name}', '#{team}', '', #{stats[0]}, #{stats[1]}, #{stats[2]}, #{stats[3]}, #{stats[4]}, #{stats[5]}, #{stats[6]}, #{stats[7]}, #{stats[8]}, #{stats[9]}, #{stats[10]}, #{stats[11]}, '#{p["opp"]}', 'FA', #{period_id});"
        pp query
        db.exec query
      else
	      query = "UPDATE fantasy SET min = #{stats[0]} WHERE player_name = '#{name}' AND period_id = #{period_id};"
        pp query
        db.exec query
      end
    end
  end
end

db.close

