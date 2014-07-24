# DAL (database abstraction layer) ![PHP 5.3+](http://img.shields.io/badge/php-5.3%2B-blue.svg)

[![Latest Unstable Version](https://poser.pugx.org/hatwebtech/dal/v/unstable.svg)](https://packagist.org/packages/hatwebtech/dal) [![License](https://poser.pugx.org/hatwebtech/dal/license.svg)](https://packagist.org/packages/hatwebtech/dal)
## Description
**dal**  is data abstraction library written in PHP. It's  lightweight (half a dozen classes) and fast. It is inspired by Doctrine
```php
//example
$q = \hatwebtech\dal\DAL::query()
        ->select('u.id, u.name')
        ->from('User u')
        ->where('u.id =?', 7)
;  
```

dal works with PHP 5.3 or later.

## Installation

The recommended way to install HATwebtech DAL is through Composer.  Just create a `composer.json` file and run the `php composer.phar install` command to install it:

```json
    {
        "require": {
            "hatwebtech/dal": "dev-master"
        }
    }
```

## Information
### Init
```php
$dsn = 'pgsql:dbname=example_db;host=127.0.0.1';

$dbh = new PDO($dsn', 'postgres', 'pass123');

\hatwebtech\dal\DAL::setDbh($dbh);
\hatwebtech\dal\DAL::setTablePath(APPPATH . 'tables/');
\hatwebtech\dal\DAL::setTableNamespace('\\dal_test\\tables\\'); 
```

### DAL query
coming soon...

### Known Issues

If you discover any bugs, feel free to create an issue on GitHub fork and send us a pull request.

* only Postgres DB is supported for now, MySQL coming soon
* implement groupBy() in Hat Dal Query class.
* implement having() in Hat Dal Query class.
* implement innerJoin() in Hat Dal Query class.
* implement validators (date, string leingth...).
* reduce memory footprint

[Issues List](https://github.com/hatwebtech/dal/issues).

## Authors

* Panajotis Zamos (https://github.com/panos-zamos)

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


## License

Apache 2.0