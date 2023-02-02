SELECT decode_xml_component('Hello, &quot;world&quot;!');
SELECT decode_xml_component('&lt;123&gt;');
SELECT decode_xml_component('&amp;clickhouse');
SELECT decode_xml_component('&apos;foo&apos;');
SELECT decode_xml_component('Hello, &&amp; world');
SELECT decode_xml_component('Hello, &;&amp; world');
SELECT decode_xml_component('Hello, &a;&amp; world');
SELECT decode_xml_component('Hello, &ltt;&amp; world');
SELECT decode_xml_component('Hello, &ltt&amp; world');
SELECT decode_xml_component('Hello, &t;&amp; world');

--decode numeric entities

SELECT decode_xml_component('&#32;&#33;&#34;&#35;&#36;&#37;&#38;&#39;&#40;&#41;&#42;&#43;&#44;&#45;&#46;&#47;&#48;&#49;&#50;');
SELECT decode_xml_component('&#41;&#42;&#43;&#44;&#45;&#46;&#47;&#48;&#49;&#50;&#51;&#52;&#53;&#54;&#55;&#56;&#57;&#58;&#59;&#60;');
SELECT decode_xml_component('&#61;&#62;&#63;&#64;&#65;&#66;&#67;&#68;&#69;&#70;&#71;&#72;&#73;&#74;&#75;&#76;&#77;&#78;&#79;&#80;');
SELECT decode_xml_component('&#20026;');
SELECT decode_xml_component('&#x4e3a;');
SELECT decode_xml_component('&#12345678;&apos;123');
SELECT decode_xml_component('&#x0426;&#X0426;&#x042E;&#X042e;&#x042B;&#x3131;');