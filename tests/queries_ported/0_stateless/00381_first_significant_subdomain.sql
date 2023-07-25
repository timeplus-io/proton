SELECT
    first_significant_subdomain('http://hello.canada.ca') AS canada,
    first_significant_subdomain('http://hello.congo.com') AS congo,
    first_significant_subdomain('http://pochemu.net-domena.ru') AS why;

SELECT
    first_significant_subdomain('ftp://www.yandex.com.tr/news.html'),
    first_significant_subdomain('https://www.yandex.ua/news.html'),
    first_significant_subdomain('magnet:yandex.abc'),
    first_significant_subdomain('ftp://www.yandex.co.uk/news.html'),
    first_significant_subdomain('https://api.www3.static.dev.ввв.яндекс.рф'),
    first_significant_subdomain('//www.yandex.com.tr/news.html');

SELECT
    first_significant_subdomain('http://hello.canada.c'),
    first_significant_subdomain('http://hello.canada.'),
    first_significant_subdomain('http://hello.canada'),
    first_significant_subdomain('http://hello.c'),
    first_significant_subdomain('http://hello.'),
    first_significant_subdomain('http://hello'),
    first_significant_subdomain('http://'),
    first_significant_subdomain('http:/'),
    first_significant_subdomain('http:'),
    first_significant_subdomain('http'),
    first_significant_subdomain('h'),
    first_significant_subdomain('.'),
    first_significant_subdomain(''),
    first_significant_subdomain('http://hello.canada..com'),
    first_significant_subdomain('http://hello..canada.com'),
    first_significant_subdomain('http://hello.canada.com.');
