#!/usr/bin/env ruby
require 'pg'

def insert_update(conn:, operation: 'insert', schema:, table:, data:[], update_where:)
	
	if operation == 'insert' || 'upsert'
		sql_op = 'INSERT INTO '
		set = ''
		bridge = ' values '
		where_clause = ''
	
	elsif operation == 'update'
		sql_op = 'UPDATE '
		set = ' SET '
		bridge = ' = '
		where_clause = ' WHERE ' + update_where.to_s

	else
		raise 'Undefined SQL operation - only "insert", "update", and "upsert" are supported.'
		return
	
	end

	begin
		uniqnum = (rand * 1000000).to_int.to_s	
		 
		query = 'SELECT ' + "'\"'" + '|| column_name ||' + "'\"'"  + "FROM information_schema.columns WHERE table_schema = '#{schema}' AND table_name = '#{table}' AND column_default IS NULL"
		conn.prepare('select_' + uniqnum, query)
		columns = conn.exec_prepared('select_' + uniqnum)
		conn.exec("DEALLOCATE " + 'select_' + uniqnum); nil

		i = 1
		params = []
		columns.each { params.push(i); i+=1; }
		columns = columns.values.join(', ')

		query = sql_op + schema.to_s + '.' + table.to_s + set + '(' + columns + ')' + bridge + '($'+params.join(', $')+')' + where_clause
		conn.prepare(operation.to_s + '_' + uniqnum, query)
		conn.exec_prepared(operation.to_s + '_' + uniqnum, data)
		
		conn.exec("DEALLOCATE " + operation.to_s + '_' + uniqnum); nil

    #Reconnect
	rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
		begin
           	conn.reset
      	rescue
            sleep 10
            retry
        end
        retry		

	rescue PG::UniqueViolation => pg_violation
  		$stderr.puts "Encountered exception #{pg_violation.message}".blue
  		if 	operation == 'upsert'
  			sql_op = 'UPDATE '
			set = ' SET '
			bridge = ' = '
			where_clause = ' WHERE ' + update_where.to_s
			retry			
		end

	rescue PG::InvalidSqlStatementName => p_inv_statement_error
	#Retry insert
		retry
					        
	rescue PG::DuplicatePstatement => p_dup_statement_error
		retry
	end
	
end



	            				
				
