/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.algebra;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestRelationList {
  @Test
  public void testEqualsTo() {
    Relation rel1 = new Relation("rel1");
    Relation rel2 = new Relation("rel2");

    RelationList relList1 = new RelationList(new Relation[]{rel1, rel2});

    Relation rel3 = new Relation("rel2");
    Relation rel4 = new Relation("rel1");

    RelationList relList2 = new RelationList(new Relation[]{rel3, rel4});

    assertTrue(relList1.equalsTo(relList2));
  }
}
