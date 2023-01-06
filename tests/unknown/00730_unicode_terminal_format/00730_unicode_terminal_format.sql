SET query_mode='table';
SET asterisk_include_reserved_columns=false;
DROP STREAM IF EXISTS unicode;

create stream unicode(c1 string, c2 string) ;
INSERT INTO unicode(c1,c2) VALUES ('Здравствуйте', 'Этот код можно отредактировать и запустить!');
INSERT INTO unicode(c1,c2) VALUES ('你好', '这段代码是可以编辑并且能够运行的！');
INSERT INTO unicode(c1,c2) VALUES ('Hola', '¡Este código es editable y ejecutable!');
INSERT INTO unicode(c1,c2) VALUES ('Bonjour', 'Ce code est modifiable et exécutable !');
INSERT INTO unicode(c1,c2) VALUES ('Ciao', 'Questo codice è modificabile ed eseguibile!');
INSERT INTO unicode(c1,c2) VALUES ('こんにちは', 'このコードは編集して実行出来ます！');
INSERT INTO unicode(c1,c2) VALUES ('안녕하세요', '여기에서 코드를 수정하고 실행할 수 있습니다!');
INSERT INTO unicode(c1,c2) VALUES ('Cześć', 'Ten kod można edytować oraz uruchomić!');
INSERT INTO unicode(c1,c2) VALUES ('Olá', 'Este código é editável e executável!');
INSERT INTO unicode(c1,c2) VALUES ('Chào bạn', 'Bạn có thể edit và run code trực tiếp!');
INSERT INTO unicode(c1,c2) VALUES ('Hallo', 'Dieser Code kann bearbeitet und ausgeführt werden!');
INSERT INTO unicode(c1,c2) VALUES ('Hej', 'Den här koden kan redigeras och köras!');
INSERT INTO unicode(c1,c2) VALUES ('Ahoj', 'Tento kód můžete upravit a spustit');
INSERT INTO unicode(c1,c2) VALUES ('Tabs \t Tabs', 'Non-first \t Tabs');
INSERT INTO unicode(c1,c2) VALUES ('Control characters \x1f\x1f\x1f\x1f with zero width', 'Invalid UTF-8 which eats pending characters \xf0, or invalid by itself \x80 with zero width');
INSERT INTO unicode(c1,c2) VALUES ('Russian ё and ё ', 'Zero bytes \0 \0 in middle');
select sleep(3);
SELECT * FROM unicode SETTINGS max_threads = 1 FORMAT PrettyNoEscapes;
SELECT 'Tabs \t Tabs', 'Long\tTitle' FORMAT PrettyNoEscapes;

SELECT '你好', '世界' FORMAT Vertical;
SELECT 'Tabs \t Tabs', 'Non-first \t Tabs' FORMAT Vertical;
SELECT 'Control characters \x1f\x1f\x1f\x1f with zero width', 'Invalid UTF-8 which eats pending characters \xf0, and invalid by itself \x80 with zero width' FORMAT Vertical;
SELECT 'Russian ё and ё', 'Zero bytes \0 \0 in middle' FORMAT Vertical;

DROP STREAM IF EXISTS unicode;
