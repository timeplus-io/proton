SELECT extract_text_from_html('<script>Here is script.</script>');
SELECT extract_text_from_html('<style>Here is style.</style>');
SELECT extract_text_from_html('<![CDATA[Here is CDTATA.]]>');
SELECT extract_text_from_html('This is a     white   space test.');
SELECT extract_text_from_html('This is a complex test. <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"\n "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"><html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en"><![CDATA[<script type="text/javascript">Hello, world</script> ]]><hello />world<![CDATA[ <style> ]]> hello</style>\n<script><![CDATA[</script>]]>hello</script>\n</html>');

DROP STREAM IF EXISTS defaults;
CREATE STREAM defaults
(
    stringColumn string
) ENGINE = Memory();

INSERT INTO defaults values ('<common tag>hello, world<tag>'), ('<script desc=content> some content </script>'), ('<![CDATA[hello, world]]>'), ('white space    collapse');

SELECT extract_text_from_html(stringColumn) FROM defaults;
DROP stream defaults;
