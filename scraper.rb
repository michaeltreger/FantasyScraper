require 'wombat'
require 'pp'
require 'sqlite3'

db = SQLite3::Database.new "fantasy.db"
db.execute "CREATE TABLE IF NOT EXISTS fantasy (player_name TEXT, fteam INT, fgm INT, fga INT, ftm INT, fta INT, reb INT, ast INT, stl INT, blk INT, tover INT, pts INT, fpts INT, opp TEXT, slot TEXT, period_id INT)"

opening_night = Time.parse("30/10/2012")
one_day = 60*60*24
current_period = ((Time.now - opening_night) / one_day).to_i

last_update = (db.execute "SELECT MAX(period_id) FROM fantasy;")[0][0]+1

(1..8).each do |team_id|
  (last_update..current_period).each do |period_id|
    data = Wombat.crawl do  
      base_url "http://games.espn.go.com"
      path "/fba/clubhouse?leagueId=202659&teamId=#{team_id}&seasonId=2013&scoringPeriodId=#{period_id}"

      players "css=.pncPlayerRow", :iterator do
        name  "css=td.playertablePlayerName a"
        opp   "css=td div a"
        slot  "css=td.playerSlot"
        stats "css=td.playertableStat", :list
      end
    end
    data["players"].each do |p|
      stats = p["stats"]
      if stats[0] == '--'
        next
      end
      query = "INSERT INTO fantasy VALUES ('#{p["name"]}', #{team_id}, #{stats[0]}, #{stats[1]}, #{stats[2]}, #{stats[3]}, #{stats[4]}, #{stats[5]}, #{stats[6]}, #{stats[7]}, #{stats[8]}, #{stats[9]}, #{stats[10]}, '#{p["opp"]}', '#{p["slot"]}', #{period_id});"
      pp query
      db.execute query
    end
  end      
end

db.close