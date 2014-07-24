<?php
namespace hatwebtech\dal;
/**
 * Mysql specific query parts
 *
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


 */
class Mysql extends DalQuery {
  protected function _parseQueryParts_limit_offset(){
    $limit = ' --- MySql limit sql --- ';
    $this->_sql_query_parts['limit'][] = $limit;
    return true;
  }
}
?>