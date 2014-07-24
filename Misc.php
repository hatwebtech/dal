<?php
/**
 * Description of DAL_query
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
class Misc {


  /*
   *
   * GENERATE TABLE CLASSES (for HDM) START
   *
   */
  private function _toClassName($table_name){
    $class_name = '';
    $name_parts = \explode('_', $table_name);
    foreach($name_parts as $name_part){
      $class_name .= \ucfirst($name_part);
    }
    return $class_name;
  }

  private $_db_info;
  private function __load_db_info(){
    if(!isset($this->_db_info)){
      $this->_db_info = array();

      $t = 'information_schema.constraint_column_usage';
      $sql = "select t.* from $t t where t.table_schema = 'public'";
      $q = \hat\dal\DAL::query();
      $q->queryStmt($sql);
      $r = $q->getResultsDbRow();
      if(isset($r[0])){
        $dbg['ISColumnConstraints'] = $r;
      }else{
        \hat\dbg::alert('ERROR : No info for ' . $t);
      }

      $t = 'information_schema.columns';
      $sql = "select t.* from $t t where t.table_schema = 'public'";
      $q = \hat\dal\DAL::query();
      $q->queryStmt($sql);
      $r = $q->getResultsDbRow();
      if(isset($r[0])){
        $dbg['ISColumns'] = $r;
      }else{
        \hat\dbg::alert('ERROR : No info for ' . $t);
      }

      $t = 'information_schema.referential_constraints';
      $sql = "select t.* from $t t where t.constraint_schema = 'public'";
      $q = \hat\dal\DAL::query();
      $q->queryStmt($sql);
      $r = $q->getResultsDbRow();
      if(isset($r[0])){
        $dbg['ISConstraints'] = $r;
      }else{
        \hat\dbg::alert('ERROR : No info for ' . $t);
      }

      $t = 'information_schema.key_column_usage';
      $sql = "select t.* from $t t where t.table_schema = 'public'";
      $q = \hat\dal\DAL::query();
      $q->queryStmt($sql);
      $r = $q->getResultsDbRow();
      if(isset($r[0])){
        $dbg['ISKeyColumnUsage'] = $r;
      }else{
        \hat\dbg::alert('ERROR : No info for ' . $t);
      }

      $t = 'information_schema.table_constraints'; // constraint_schema
      $sql = "select t.* from $t t where t.constraint_schema = 'public'";
      $q = \hat\dal\DAL::query();
      $q->queryStmt($sql);
      $r = $q->getResultsDbRow();
      if(isset($r[0])){
        $dbg['ISTableConstraints'] = $r;
      }else{
        \hat\dbg::alert('ERROR : No info for ' . $t);
      }

      $t = 'pg_catalog.pg_indexes';
      $sql = "select t.* from $t t ";
      $q = \hat\dal\DAL::query();
      $q->queryStmt($sql);
      $r = $q->getResultsDbRow();
      if(isset($r[0])){
        $dbg['PGIndexes'] = $r;
      }else{
        \hat\dbg::alert('ERROR : No info for ' . $t);
      }

      $this->_db_info = $dbg;


    }
    return $this->_db_info;
  }
  private function __load_table_info($table_name){

    $sql = "select t.* from information_schema.tables t  where t.table_schema = 'public' and t.table_name = ?";
    $q = \hat\dal\DAL::query();
    $q->setBindParam(array('type' => 'string', 'value' => $table_name));
    $q->queryStmt($sql);
    $dbg['tables'] = $q->getResultsDbRow();
    if(isset($dbg['tables'][0])){
      return $dbg['tables'][0];
    }else{
      \hat\dbg::alert('ERROR : No table info for table ' . $table_name);
      return false;
    }
  }

  function listTables(){
    $sql = "select t.table_name from information_schema.tables t  where t.table_schema = 'public' order by t.table_name";
    $q = \hat\dal\DAL::query();
    $q->queryStmt($sql);
    $dbg['tables'] = $q->getResultsDbRow();
    $tables = array();
    foreach($dbg['tables'] as $t){
      //echo "    '{$t['table_name']}',\n";
      $tables[] = $t['table_name'];
    }
    //\hat\dbg::alert($dbg);
    return $tables;
  }

  private function _generateTableClass($table_name){

    $class_name = $this->_toClassName($table_name);
    $path = \hat\dal\DAL::getTablePath();
    //\hat\dbg::alert($path);
    $namespace = \hat\dal\DAL::getTableNamespace();
    $namespace = \ltrim($namespace, '\\');
    $namespace = \rtrim($namespace, '\\');
    //\hat\dbg::alert($namespace, true);
    if(\is_writable($path)){
      $filename = $path . '/' . $class_name . '.php';
    }else{
      $filename = '/tmp/' . $class_name . '.php';
    }
    if(\file_exists($filename)){
      \hat\dbg::alert("PHP class for $class_name exists!");
      return;
    }else{
      \hat\dbg::alert("genarating PHP class for $class_name.");
    }


    $db_info = $this->__load_db_info();
    //\hat\dbg::alert($db_info, true);
    $table_info = $this->__load_table_info($table_name);
    if($table_info == false){
      return false;
    }

    $ISTables[$table_name] = $table_info;
    $PGIndexes = $db_info['PGIndexes'];
    $ISColumns = $db_info['ISColumns'];
    $ISTableConstraints = $db_info['ISTableConstraints'];
    //$ISTableConstraints = $db_info['ISTableConstraints'];
    $ISColumnConstraints = $db_info['ISColumnConstraints'];
    $ISConstraints = $db_info['ISConstraints'];
    $ISKeyColumnUsage = $db_info['ISKeyColumnUsage'];

    $results = array();
    foreach($ISTables as $t_name => $t){
      $columns = array();
      foreach($ISColumns as $c){
        if($c['table_name'] == $t_name){
          $columns[$c['column_name']] = $c;
        }
      }
      $ISTables[$t_name]['Columns'] = $columns;
    }
    foreach($ISTables as $t_name => $t){
      $indexes = array();
      $pkeys = array();
      foreach($PGIndexes as $index){
        if($index['tablename'] == $t_name){
          $indexes[] = $index;
        }
        if($index['indexname'] == $t_name . '_pkey'){
          $string = $index['indexdef'];
          $start = \strpos($string, '(') +1;
          $stop = \strpos($string, ')');
          $length = $stop - $start;
          $_pkeys_string = \substr($string, $start, $length);
          $pkeys = \array_map('trim', \explode(',', $_pkeys_string));
          //\hat\dbg::alert($pkeys, true);
        }
      }
      $ISTables[$t_name]['Indexes'] = $indexes;
      if(isset($pkeys)){
        $ISTables[$t_name]['PKeys'] = $pkeys;
      }
    }
    //return $ISTables;
    foreach($ISTables as $t_name => $t){
      $pkeys = array();
      $fkeys = array();
      $rkeys = array();
      $referenced_by = array();
      $used_constraints = array();
      foreach($ISTableConstraints as $tc){
        if($tc['table_name'] == $t_name){
          foreach($ISKeyColumnUsage as $v0){
            if($tc['constraint_name'] == $v0['constraint_name']){
              $used_constraints[] = $tc['constraint_name'];
              $tc['Constraint']['v0__'] = $v0;
            }
          }
          foreach($ISConstraints as $constraint){
            if($tc['constraint_name'] == $constraint['constraint_name']){
              $used_constraints[] = $tc['constraint_name'];
              $tc['Constraint'] = $constraint;

              foreach($ISKeyColumnUsage as $v2){
                if($constraint['constraint_name'] == $v2['constraint_name']){
                  $tc['Constraint']['v2_1'] = $v2;
                }
                if($constraint['unique_constraint_name'] == $v2['constraint_name']){
                  $tc['Constraint']['v2_2'] = $v2;
                }
              }
            }
          }
          if($tc['constraint_type'] == 'PRIMARY KEY'){
            $pkeys[] = $tc;
          }elseif($tc['constraint_type'] == 'FOREIGN KEY'){
            $fkeys[] = $tc;
          }else{
            $rkeys[] = $tc;
          }
          $referenced_by = array();
          foreach($ISColumnConstraints as $cc){
            if($t_name == $cc['table_name']){
              if(!\in_array($cc['constraint_name'], $used_constraints)){
                foreach($ISKeyColumnUsage as $v3){
                  if($cc['constraint_name'] == $v3['constraint_name']){

                    $cc['Constraint']['v0__'] = $v3;
                    foreach($ISConstraints as $constraint){
                      if($cc['constraint_name'] == $constraint['constraint_name']){
                        $used_constraints[] = $tc['constraint_name'];
                        $cc['Constraint'] = $constraint;

                        foreach($ISKeyColumnUsage as $v2){
                          if($constraint['constraint_name'] == $v2['constraint_name']){
                            $cc['Constraint']['v2_1'] = $v2;
                          }
                          if($constraint['unique_constraint_name'] == $v2['constraint_name']){
                            $cc['Constraint']['v2_2'] = $v2;
                          }
                        }
                      }
                    }


                  }
                }
                $referenced_by[] = $cc;
              }
            }
          }

        }
      }
      $ISTables[$t_name]['Constraints'] = array('PRIMARY_KEY' => $pkeys, 'FOREIGN_KEY' => $fkeys, 'Referenced by' => $referenced_by, 'OTHER_KEYS' => $rkeys);
    }


     /*
         *  GENERATE PHP
         */

    $php = "<?php
/**
 * @author created by HDM table class generator
 */
namespace $namespace;
class $class_name extends \hat\dal\Table{
  public function setTableDefinition(){
    \$this->setTableName('$table_name');
";


    foreach($ISTables as $t_name => $t_info){
      foreach($t_info['Columns'] as $key => $column){
        if($column['data_type'] == 'character varying'){
          $_type = "string";
          $_size = $column['character_maximum_length'];
        }elseif($column['data_type'] == 'bigint'){
          $_type = "integer";
          $_size = '8';
        }elseif($column['data_type'] == 'boolean'){
          $_type = "boolean";
          $_size = '1';
        }elseif($column['data_type'] == 'timestamp without time zone'){
          $_type = "timestamp";
          $_size = 'null';
        }else{
          $_type = $column['data_type'];
          $_size = '0';
        }
        $_options = 'array(';
        if(isset($t_info['PKeys']) && \in_array($key, $t_info['PKeys'])){
          $_options .= '\'primary\' => true';
        }
        $_options .= ')';
        $php .= "    \$this->hasColumn('$key', '$_type', $_size, $_options);\n";
      }

    $php .= "  }

  public function setUp(){
    parent::setUp();
    //\$this->actAs('');

";

//    \hat\dbg::alert($t_info);
//    \hat\dbg::alert($php, true);
//      $pkeys = array();
//      $_pkeys = array();
//      foreach($t_info['Constraints']['PRIMARY_KEY'] as $c){
//        if(isset($c['Constraint']['v0__'])){
//          $pkeys[] = $c['Constraint']['v0__']['column_name'];
//          $_pkeys[] = $c['constraint_name'];
//        }
//      }
//      if(empty($fkeys)){
//        echo "  Primary keys ( NONE )$nl";
//      }else{
//        echo "  Primary keys ("; echo(\implode(', ', $pkeys)); echo ")$nl";
//      }
//      $indexes = array();
//      foreach($t_info['Indexes'] as $i){
//        $_i = "    --  '{$i['indexname']}' ";
//        if(\in_array($i['indexname'], $_pkeys)){
//          $_i .= "PRIMARY KEY, ";
//        }
//        $_i .= \substr($i['indexdef'], \strpos($i['indexdef'], 'USING') + 5);
//        $indexes[] = $_i;
//      }
//      if(empty($indexes)){
//        echo "  Indexes: $nl    --  NONE$nl";
//      }else{
//        echo "  Indexes: $nl"; echo(\implode(",$nl", $indexes)); echo $nl;
//      }
      $fkeys = array();
      foreach($t_info['Constraints']['FOREIGN_KEY'] as $c){
        $fk = "    --  '{$c['Constraint']['constraint_name']}' FOREIGN KEY ({$c['Constraint']['v2_1']['column_name']}) REFERENCES {$c['Constraint']['v2_2']['table_name']}({$c['Constraint']['v2_2']['column_name']}) ";
        //$fk = "    --  {$c['Constraint']['v2_1']['column_name']} REFERENCES {$c['Constraint']['v2_2']['table_name']}({$c['Constraint']['v2_2']['column_name']}) ";
        if($c['Constraint']['update_rule'] != 'NO ACTION'){
          $fk .= '  ON UPDATE ' . $c['Constraint']['update_rule'];
        }
        if($c['Constraint']['delete_rule'] != 'NO ACTION'){
          $fk .= '  ON DELETE ' . $c['Constraint']['delete_rule'];
        }
        $fkeys[] = $fk;

        $_c = $this->_toClassName($c['Constraint']['v2_2']['table_name']);
        $_l = $c['Constraint']['v2_1']['column_name'];
        $_f = $c['Constraint']['v2_2']['column_name'];

        $php .= "    \$this->hasOne('$_c', array('local' => '$_l', 'foreign' => '$_f'));\n";

      }
//      if(empty($fkeys)){
//        echo "  Foreign keys: $nl    --  NONE$nl";
//      }else{
//        echo "  Foreign keys: $nl"; echo(\implode(",$nl", $fkeys)); echo $nl;
//      }


      $ref_by = array();
      foreach($t_info['Constraints']['Referenced by'] as $c){
        $rb = "    --  TABLE {$c['Constraint']['v2_1']['table_name']} CONSTRAINT '{$c['Constraint']['constraint_name']}' FOREIGN KEY ({$c['Constraint']['v2_1']['column_name']}) REFERENCES {$c['Constraint']['v2_2']['table_name']}({$c['Constraint']['v2_2']['column_name']}) ";
        //$rb = "    --  TABLE {$c['Constraint']['v2_1']['table_name']}({$c['Constraint']['v2_1']['column_name']}) REFERENCES {$c['Constraint']['v2_2']['column_name']}";
        if($c['Constraint']['update_rule'] != 'NO ACTION'){
          $rb .= '  ON UPDATE ' . $c['Constraint']['update_rule'];
        }
        if($c['Constraint']['delete_rule'] != 'NO ACTION'){
          $rb .= '  ON DELETE ' . $c['Constraint']['delete_rule'];
        }
        $ref_by[] = $rb;

        $_c = $this->_toClassName($c['Constraint']['v2_1']['table_name']);
        $_l = $c['Constraint']['v2_1']['column_name'];
        $_f = $c['Constraint']['v2_2']['column_name'];

        $php .= "    \$this->hasMany('$_c', array('local' => '$_f', 'foreign' => '$_l'));\n";

      }
//      if(empty($ref_by)){
//        echo "  Referenced by: $nl    --  NONE$nl";
//      }else{
//        echo "  Referenced by: $nl"; echo(\implode(",$nl", $ref_by)); echo "$nl";
//      }
  
      // close
      $php .= "  }
}
?>";


    }


    $fileHandle = fopen($filename, 'w+') or die("can't open file");
    $b = fwrite($fileHandle, $php);
    fclose($fileHandle);
    \chmod($filename, 0666);
    \hat\dbg::alert("written $b bytes in $class_name [path = $filename].");

//    echo "<pre>\n\nPHP code:\n";
//    echo $php;
//    \hat\dbg::alert('end', true);


  }
  function generateTableClass($table_names = null){
    if($table_names){
      if(is_string($table_names)){
        $table_names = array($table_names);
      }
    }
    if(!is_array($table_names)){
      return;
    }
    
//    $dbg = array(
////        'post' => $_POST,
//        'data' => $this->data,
////        'files' => $_FILES,
//    );
//    //\hat\dbg::alert($dbg);
//    $table_names_str = isset($this->data['table_names']) ? $this->data['table_names'] : '';
//    $table_names = empty($table_names_str) ? array() : \explode("\n", $table_names_str);
//    \hat\dbg::alert($table_names);
//
//    $html = '
//    <form method="post" action="/en/api/generateTableClass">
//        <label for="table_names">table names</label><br/>
//        <textarea rows="5" cols="60" id="table_names" name="table_names"></textarea><br/>
//        <input type="submit" value="submit" /><br/>
//    </form>
//    ';
//    echo $html;//exit;


    foreach($table_names as $table_name){
      $this->_generateTableClass(\trim($table_name));
    }
  }
  /*
   *
   * GENERATE TABLE CLASSES (for HDM) END
   *
   */

}

?>
