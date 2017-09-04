#-------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------

# junit test class: org.apache.sysml.test.integration.functions.binary.scalar.XorTest.java

$$readhelper$$

trueVar = TRUE;

Left1 = Helper;
i = 0;
j = 0;
while(i == 0 ^ j == 0) //1 ^ 1 = 0
{
	Left1 = 2 * Left1;
	i = 1;
	j = 1;
}
write(Left1, "$$outdir$$left_1", format="text");

Left2 = Helper;
i = 0;
while(i == 0 ^ trueVar == TRUE) //1 ^ 0 = 1
{
  left2 = 2 * Left2;
  i = 1;
}
write(Left2, "$$outdir$$left_2", format="text");

Left3 = Helper;
i = 0;
j = 0;
while(i == 1 ^ j == 0) //0 ^ 1 = 1
{
  left3 = 2 * left3;
  j = 1;
}
write(Left3, "$$outdir$$left_3", format="text");

Left4 = Helper;
i = 0;
while(i == 1 ^ trueVar == FALSE) //0 ^ 0 = 0
{
  Left4 = 2 * Left4;
}
write(Left4, "$$outdir$$left_4", format="text");

Right1 = Helper;
i = 0;
j = 0;
while(j == 0 ^ i == 0) //1 ^ 1 = 0 
{
  Right1 = 2 * Right1;
  i = 1;
  j = 1;
}
write(Right1, "$$outdir$$right_1", format="text");

Right2 = Helper;
i = 0;
j = 0;
while(j == 0 ^ i == 1) //1 ^ 0 = 1
{
  Right2 = 2 * Right2;
  j = 1;
}
write(Right2, "$$outdir$$right_2", format="text");

Right3 = Helper;
i = 0;
while(trueVar == TRUE ^ i == 0) //0 ^ 1 = 1
{
  Right3 = 2 * Right3;
  i = 1;
}
write(Right3, "$$outdir$$right_3", format="text");

Right4 = Helper;
i = 0;
while(trueVar == FALSE ^ i == 1) //0 ^ 0 = 0
{
  Right4 = 2 * Right4;
}
write(Right4, "$$outdir$$right_4", format="text");
