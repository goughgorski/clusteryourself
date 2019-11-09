#!/usr/bin/env ruby

def extract_pdf_text(pdf)
  text = Henkei.read :text, pdf
rescue Exception => error
  Rails.logger.fatal("Failed to extract text from PDF\n#{error.class} (#{error.message})")
  nil
end

def extract_ocr_text pdf
  engine = Tesseract::Engine.new{|e| e.language= :eng}
  tempdir = Dir.mktmpdir
  begin
    IO.popen("convert -density 300 -[0] -depth 8 #{tempdir}/page-%04d.png", "w", encoding: 'BINARY', err: :close) { |p| p.write pdf }
    Dir.glob("#{tempdir}/page-*.png").map { |page| engine.text_for page }.join("\n")
  ensure
    FileUtils.rm_r tempdir
  end
end