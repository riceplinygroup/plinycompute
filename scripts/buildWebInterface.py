#!/usr/bin/python3 -u
#  ========================================================================
#  Copyright 2018 Rice University
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  ========================================================================

import shutil
import sys
import os

if shutil.which("npm") is None:
    print("Will not build the web interface. Install 'npm'.")
    sys.exit(1)

# check if the files are already generated if the haven not generate them
if not os.path.exists('node_modules'):
    os.system('npm install')