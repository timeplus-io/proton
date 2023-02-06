-- { echo }

SELECT extract_text_from_html('');
SELECT extract_text_from_html(' ');
SELECT extract_text_from_html('  ');
SELECT extract_text_from_html('Hello');
SELECT extract_text_from_html('Hello, world');
SELECT extract_text_from_html('Hello,  world');
SELECT extract_text_from_html(' Hello,  world');
SELECT extract_text_from_html(' Hello,  world ');
SELECT extract_text_from_html(' \t Hello,\rworld \n ');

SELECT extract_text_from_html('Hello<world');
SELECT extract_text_from_html('Hello < world');
SELECT extract_text_from_html('Hello > world');
SELECT extract_text_from_html('Hello<world>');
SELECT extract_text_from_html('Hello<>world');
SELECT extract_text_from_html('Hello<!>world');
SELECT extract_text_from_html('Hello<!->world');
SELECT extract_text_from_html('Hello<!-->world');
SELECT extract_text_from_html('Hello<!--->world');
SELECT extract_text_from_html('Hello<!---->world');

SELECT extract_text_from_html('Hello <!-- --> World');
SELECT extract_text_from_html('Hello<!-- --> World');
SELECT extract_text_from_html('Hello<!-- -->World');
SELECT extract_text_from_html('Hello <!-- -->World');
SELECT extract_text_from_html('Hello <u> World</u>');
SELECT extract_text_from_html('Hello <u>World</u>');
SELECT extract_text_from_html('Hello<u>World</u>');
SELECT extract_text_from_html('Hello<u> World</u>');

SELECT extract_text_from_html('<![CDATA[ \t Hello,\rworld \n ]]>');
SELECT extract_text_from_html('Hello <![CDATA[Hello\tworld]]> world!');
SELECT extract_text_from_html('Hello<![CDATA[Hello\tworld]]>world!');

SELECT extract_text_from_html('Hello <![CDATA[Hello <b>world</b>]]> world!');
SELECT extract_text_from_html('<![CDATA[<sender>John Smith</sender>]]>');
SELECT extract_text_from_html('<![CDATA[<sender>John <![CDATA[Smith</sender>]]>');
SELECT extract_text_from_html('<![CDATA[<sender>John <![CDATA[]]>Smith</sender>]]>');
SELECT extract_text_from_html('<![CDATA[<sender>John ]]><![CDATA[Smith</sender>]]>');
SELECT extract_text_from_html('<![CDATA[<sender>John ]]> <![CDATA[Smith</sender>]]>');
SELECT extract_text_from_html('<![CDATA[<sender>John]]> <![CDATA[Smith</sender>]]>');
SELECT extract_text_from_html('<![CDATA[<sender>John ]]>]]><![CDATA[Smith</sender>]]>');

SELECT extract_text_from_html('Hello<script>World</script> goodbye');
SELECT extract_text_from_html('Hello<script >World</script> goodbye');
SELECT extract_text_from_html('Hello<scripta>World</scripta> goodbye');
SELECT extract_text_from_html('Hello<script type="text/javascript">World</script> goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World</style> goodbye');
SELECT extract_text_from_html('Hello<script:p>World</script:p> goodbye');
SELECT extract_text_from_html('Hello<script:p type="text/javascript">World</script:p> goodbye');

SELECT extract_text_from_html('Hello<style type="text/css">World <!-- abc --> </style> goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World <!-- abc --> </style \n > goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World <!-- abc --> </ style> goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World <!-- abc --> </stylea> goodbye');

SELECT extract_text_from_html('Hello<style type="text/css">World <![CDATA[</style>]]> </stylea> goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World <![CDATA[</style>]]> </style> goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World <![CDAT[</style>]]> </style> goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World <![endif]--> </style> goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World <script>abc</script> </stylea> goodbye');
SELECT extract_text_from_html('Hello<style type="text/css">World <script>abc</script> </style> goodbye');

SELECT extract_text_from_html('<![CDATA[]]]]><![CDATA[>]]>');

SELECT extract_text_from_html('
<img src="pictures/power.png" style="margin-bottom: -30px;" />
<br><span style="padding-right: 10px; font-size: 10px;">xkcd.com</span>
</div>
');
