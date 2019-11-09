#!/usr/bin/env ruby

def representation_for_transaction(transaction) 
  if transaction.nil?
    :unknown
  elsif transaction.agent_represents_buyer? && transaction.agent_represents_seller?
    :both
  elsif transaction.agent_represents_buyer?
    :buyer
  elsif transaction.agent_represents_seller?
    :seller
  else
    :unknown
  end
end