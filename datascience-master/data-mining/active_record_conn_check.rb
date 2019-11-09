#!/usr/bin/env ruby

def active_record_conn_check

	#Check connection and get addtional information
	$stderr.puts 'Checking Active Record connection.'
	
	begin
		
		db = ActiveRecord::Base.connection.current_database; nil
		
		if db.nil?
			
			begin
				
				$stderr.puts 'No connection found. Attempting to reestabish.'
				ActiveRecord::Base.connection.reconnect!
			
			rescue
				
				sleep 10
				retry
			
			end
		
		end

		db = ActiveRecord::Base.connection.current_database; nil
		
		if db.nil?
			
			return false
		
		else
			
			return true
		
		end
	
	end

end
