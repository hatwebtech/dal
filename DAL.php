<?php
/**
 * Description of DAL [Database Abstraction Layer]
 *
 * Packagist hook test :)
 *
    Copyright 2011-2014 hatwebtech.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 *
 * @license Apache License, Version 2.0  http://www.apache.org/licenses/LICENSE-2.0
 *
 * @author Panajotis Zamos
 */
namespace hat\dal;
abstract class DAL{

  private static $_dbh;
  private static $_dbhs;
  private static $_table_path;
  private static $_table_namespace;
  private static $_namespace_separator = '\\';
  private static $_tables;
  private static $_no_behavior;
  private static $_clone_tables;

  const AS_ARRAY = 1;
  const AS_OBJECT = 2;
  const AS_DB_ROW = 3;
  const PARSE_TREE_RESULTS = true;
  const DO_NOT_PARSE_TREE_RESULTS = false;

  /**
   *
   * @param PDO object $dbh
   */
  public static function setDbh($dbh){
    self::$_dbh = $dbh;
    spl_autoload_register(array('\hat\dal\DAL', '_hdm_loader'));
  }
  public static function getDbh(){
    return self::$_dbh;
  }

  /**
   *
   * @param string $className
   */
  public static function _hdm_loader($className){
    $_namespaceSeparator = self::$_namespace_separator;
    $_fileExtension = '.php';

    $fileName = '';
    $namespace = '';
    if (false !== ($lastNsPos = strripos($className, $_namespaceSeparator))) {
        $namespace = substr($className, 0, $lastNsPos);
        $className = substr($className, $lastNsPos + 1);
        $fileName = str_replace($_namespaceSeparator, DIRECTORY_SEPARATOR, $namespace) . DIRECTORY_SEPARATOR;
    }

    //print_r(\compact('namespace', 'className', 'fileName'));

    if($namespace == 'hat\dal'){
      require __DIR__ . \DIRECTORY_SEPARATOR . $className . $_fileExtension;
    }elseif(isset(self::$_table_namespace) && isset(self::$_table_path) && $_namespaceSeparator . $namespace . $_namespaceSeparator == self::$_table_namespace){
      require self::$_table_path . \DIRECTORY_SEPARATOR . $className . $_fileExtension;
    }

  }

  /**
   *
   * @param string $separator
   */
  public static function setNamespaceSeparator($separator){
    self::$_namespace_separator = $separator;
  }
  /**
   *
   * @return string
   */
  public static function getNamespaceSeparator(){
    return self::$_namespace_separator;
  }

  /**
   *
   * @param string $path
   */
  public static function setTablePath($path){
    self::$_table_path = $path;
  }
  /**
   *
   * @return string
   */
  public static function getTablePath(){
    return self::$_table_path;
  }
  /**
   *
   * @param string $namespace
   */
  public static function setTableNamespace($namespace){
    self::$_table_namespace = $namespace;
  }
  /**
   *
   * @return string
   */
  public static function getTableNamespace(){
    return self::$_table_namespace;
  }
/**
 *
 * @return DalQuery
 */
  public static function query(){
    $driver_name = self::$_dbh->getAttribute(\PDO::ATTR_DRIVER_NAME);
    if($driver_name == 'pgsql'){
      $query = new \hat\dal\DalQuery();
    }else{
      switch($driver_name){
        case 'mysql' : $query = new \hat\dal\Mysql(); break;
      }
      if(!isset($query)){
        exit("db driver $driver_name not suported by HDM.");
      }
    }
    $query->setDbh(self::$_dbh);
    $query->setTablePath(self::$_table_path);
    $query->setTableNamespace(self::$_table_namespace);
    if(self::$_no_behavior){
      $query->noBehavior();
    }
    return $query;
  }
  public static function noBehavior(){
    self::$_no_behavior = true;
  }
  public static function withBehavior(){
    self::$_no_behavior = false;
  }

  /**
   *
   * @param string $model_name
   * @return Table
   */
  public static function load($model_name){
    if(isset(self::$_tables[$model_name])){
//      return clone self::$_tables[$model_name];
      $model_class_name = self::$_table_namespace . $model_name;
      $model = new $model_class_name();
      return $model;
    }
    $model_class_name = self::$_table_namespace . $model_name;
    $model = new $model_class_name();
//      self::$_tables[$model_name] = $model;
    self::$_tables[$model_name] = true;
    return $model;
//    throw new \Exception("Module not found for $model_name in ".self::$_table_path.'.');
//    return false;
  }
  /**
   *
   * @param string $model_name
   * @return Table
   */
  public static function loadClone($model_name){
    if(isset(self::$_clone_tables[$model_name])){
      return clone self::$_clone_tables[$model_name];
//      $model_class_name = self::$_table_namespace . $model_name;
//      $model = new $model_class_name();
//      return $model;
    }
    $model_class_name = self::$_table_namespace . $model_name;
    $model = new $model_class_name();
    self::$_clone_tables[$model_name] = $model;
//      self::$_tables[$model_name] = true;
    return $model;
//    throw new \Exception("Module not found for $model_name in ".self::$_table_path.'.');
//    return false;
  }
}
?>