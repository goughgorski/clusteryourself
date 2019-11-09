Rails.logger.level = :warn

  api_key=ENV.fetch("MIXPANEL_API_KEY", 'f2e400c1bb39dfe2653a021b96f9a68d')
  api_secret=ENV.fetch("MIXPANEL_API_SECRET", '43b7b4eed3203e3f7b8a731c36444b96')

  client = Mixpanel::Client.new(
    api_secret: api_secret,
    timeout: 240    # Default is 60 seconds, increase if you get frequent Net::ReadTimeout errors.
  )


	$stderr.puts "  Fetching 'scan finished' events from Mixpanel"
    event_data = client.request(
      'export',
      event:     '["scan finished"]',
      from_date: "2016-01-01",
      to_date:   Date.today.to_s(:year_month_day),
    )

   mx_user_ids = event_data.map{|data| data['properties']['distinct_id']}.uniq
   mx_user_ids2 = mx_user_ids[0..9]
   mx_user_emails = [] 
   t = Parallel.each(mx_user_ids2, in_processes: 4, progress: '  Progress') do |distinct_id|
      $stderr.puts "  Fetching user information for user #{distinct_id} from Mixpanel"
      
      begin
      	person_data = client.request(
        	'engage',
        	distinct_id: distinct_id.to_s
      	)
		puts distinct_id
		if person_data['results'].empty?
			mx_user_emails << 'Person not found'
        else 
        	email = person_data['results'].first['$properties']['$email']
        	mx_user_emails << email
        end 
      rescue => e
      	$stderr.puts "Encountered exception #{e}".red					
      end
    
    end

   $stderr.puts "  Fetching user information for #{user_count} users from Mixpanel"

    emails_of_users_who_signed_up_in_month = Parallel.map(mixpanel_user_ids_of_users_who_signed_up_in_month, in_processes: 8, progress: '  Progress') do |distinct_id|
      person_data = client.request(
        'engage',
        distinct_id: distinct_id
      )
      begin
        person_data['results'].first['$properties']['$email']
      rescue
      end
    end.compact


person_data = client.request(
        	'engage',
        	distinct_id: "37077"
      	)




  month_start = 36.months.ago.beginning_of_month.beginning_of_day
  month_end = month_start.end_of_month.end_of_day

THIS FORM HAS BEEN APPROVED BY THE FLORIDA REALTORS