/*
   Copyright (c) 2023, 2023, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef GET_CPU_USAGE_HPP
#define GET_CPU_USAGE_HPP

#define JAM_FILE_ID 49


/**
 * Get CPU measurement
 */
class GetCpuUsageReq {
  
  /**
   * Receiver(s)
   */
  friend class Thrman;
  
  /**
   * Sender
   */
  friend class Backup;
  friend class Dblqh;

public:
  STATIC_CONST( SignalLength = 1 );
public:
  enum RequestType {
    PerSecond = 0,
    LastMeasure = 1
  };
  Uint32 requestType;
};

class GetCpuUsageConf {

  /**
   * Sender(s)
   */
  friend class Thrman;
  
  /**
   * Receiver(s)
   */
  friend class Backup;
  friend class Dblqh;

public:
  STATIC_CONST( SignalLength = 4 );
  
public:
  Uint32 real_exec_time;
  Uint32 os_exec_time;
  Uint32 exec_time;
  Uint32 block_exec_time;
};
#undef JAM_FILE_ID

#endif
