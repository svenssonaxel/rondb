/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
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



#define DBACC_C
#include "util/require.h"
#include "Dbacc.hpp"

#define JAM_FILE_ID 346


#define DEBUG(x) { ndbout << "ACC::" << x << endl; }

Uint64 Dbacc::getTransactionMemoryNeed(
    const Uint32 ldm_instance_count,
    const ndb_mgm_configuration_iterator * mgm_cfg)
{
  Uint32 acc_op_reserved_recs = 0;
  Uint32 acc_op_recs = 0;

  {
    require(!ndb_mgm_get_int_parameter(mgm_cfg,
                                       CFG_LDM_RESERVED_OPERATIONS,
                                       &acc_op_reserved_recs));
    require(!ndb_mgm_get_int_parameter(mgm_cfg,
                                       CFG_ACC_OP_RECS,
                                       &acc_op_recs));
    acc_op_recs += acc_op_reserved_recs;
    acc_op_recs += (1000 * globalData.ndbMtQueryWorkers);
  }

  Uint64 op_byte_count = 0;
  op_byte_count += Operationrec_pool::getMemoryNeed(acc_op_recs);
  op_byte_count *= ldm_instance_count;
  return op_byte_count;
}

void Dbacc::initData()
{
#if defined(VM_TRACE) || defined(ERROR_INSERT)
  m_acc_mutex_locked = RNIL;
#endif
#ifdef ACC_OLD
  c_restart_allow_use_spare = true;
#endif//ACC_OLD
  m_curr_acc = this;
  ctablesize = ZTABLESIZE;

  Pool_context pc;
  pc.m_block = this;
#ifdef ACC_OLD
  if (!m_is_query_block)
  {
    directoryPool.init(RT_DBACC_DIRECTORY, pc);
    directoryPoolPtr = &directoryPool;
  }
  else
  {
    directoryPoolPtr = 0;
  }
#endif//ACC_OLD

  tabrec = 0;

#ifdef ACC_OLD
  void* ptr = m_ctx.m_mm.get_memroot();
  c_page_pool.set((Page32*)ptr, (Uint32)~0);
#endif//ACC_OLD

  c_fragment_pool.init(RT_DBACC_FRAGMENT, pc);

#ifdef ACC_OLD
  c_allow_use_of_spare_pages = false;
#endif//ACC_OLD
  cfreeopRec = RNIL;

#ifdef ACC_OLD
  cnoOfAllocatedPagesMax = cnoOfAllocatedPages = cpageCount = 0;
#endif//ACC_OLD
  // Records with constant sizes

  RSS_OP_COUNTER_INIT(cnoOfAllocatedFragrec);

}//Dbacc::initData()

void Dbacc::initRecords(const ndb_mgm_configuration_iterator *mgm_cfg) 
{
  jam();
#if defined(USE_INIT_GLOBAL_VARIABLES)
  {
    void* tmp[] = { &fragrecptr,
                    &operationRecPtr,
                    &queOperPtr,
                    &tabptr
    };
    init_global_ptrs(tmp, sizeof(tmp)/sizeof(tmp[0]));
  }
#endif
#ifdef ACC_OLD
  cfreepages.init();
  ndbassert(pages.getCount() - cfreepages.getCount() + cnoOfAllocatedPages ==
            cpageCount);
#endif//ACC_OLD

  if (m_is_query_block)
  {
    ctablesize = 0;
  }

  tabrec = (Tabrec*)allocRecord("Tabrec",
				sizeof(Tabrec),
				ctablesize);

  /**
   * Records moved into poolification is created and the
   * static part of the pool is allocated as well.
   */

  Pool_context pc;
  pc.m_block = this;

  Uint32 reserveOpRecs = 1;
  Uint32 local_acc_operations = 1;
  ndbrequire(!ndb_mgm_get_int_parameter(mgm_cfg,
            CFG_LDM_RESERVED_OPERATIONS, &reserveOpRecs));
  ndbrequire(!ndb_mgm_get_int_parameter(mgm_cfg,
            CFG_ACC_OP_RECS, &local_acc_operations));
  reserveOpRecs += local_acc_operations;
  if (m_is_query_block)
  {
    reserveOpRecs = 1000;
  }
  oprec_pool.init(
    Operationrec::TYPE_ID,
    pc,
    reserveOpRecs,
    UINT32_MAX);
  while (oprec_pool.startup())
  {
    refresh_watch_dog();
  }
  if (!m_is_query_block)
  {
    for (Uint32 i = 0; i < ZMAX_PARALLEL_COPY_FRAGMENT_OPS; i++)
    {
      ndbrequire(oprec_pool.seize(operationRecPtr));
      operationRecPtr.p->userptr = RNIL;
      operationRecPtr.p->userblockref = 0;
      m_reserved_copy_frag_lock.addFirst(operationRecPtr);
    }
  }
}//Dbacc::initRecords()

Dbacc::Dbacc(Block_context& ctx,
             Uint32 instanceNumber,
             Uint32 blockNo):
  SimulatedBlock(blockNo, ctx, instanceNumber),
  m_reserved_copy_frag_lock(oprec_pool),
  c_tup(0)
#ifdef ACC_OLD
  ,c_page8_pool(c_page_pool)
#endif//ACC_OLD
{
  BLOCK_CONSTRUCTOR(Dbacc);

  // Transit signals
  if (blockNo == DBACC)
  {
    addRecSignal(GSN_DUMP_STATE_ORD, &Dbacc::execDUMP_STATE_ORD);
    addRecSignal(GSN_DEBUG_SIG, &Dbacc::execDEBUG_SIG);
    addRecSignal(GSN_CONTINUEB, &Dbacc::execCONTINUEB);
#ifdef ACC_OLD
    addRecSignal(GSN_EXPANDCHECK2, &Dbacc::execEXPANDCHECK2);
    addRecSignal(GSN_SHRINKCHECK2, &Dbacc::execSHRINKCHECK2);
#endif//ACC_OLD

    // Received signals
    addRecSignal(GSN_STTOR, &Dbacc::execSTTOR);
    addRecSignal(GSN_ACCSEIZEREQ, &Dbacc::execACCSEIZEREQ);
    addRecSignal(GSN_ACCFRAGREQ, &Dbacc::execACCFRAGREQ);
    addRecSignal(GSN_ACC_TO_REQ, &Dbacc::execACC_TO_REQ);
    addRecSignal(GSN_ACC_LOCKREQ, &Dbacc::execACC_LOCKREQ);
    addRecSignal(GSN_NDB_STTOR, &Dbacc::execNDB_STTOR);
    addRecSignal(GSN_DROP_TAB_REQ, &Dbacc::execDROP_TAB_REQ);
    addRecSignal(GSN_READ_CONFIG_REQ, &Dbacc::execREAD_CONFIG_REQ, true);
    addRecSignal(GSN_DROP_FRAG_REQ, &Dbacc::execDROP_FRAG_REQ);

    addRecSignal(GSN_DBINFO_SCANREQ, &Dbacc::execDBINFO_SCANREQ);
    m_is_query_block = false;
    m_is_in_query_thread = false;
    m_lqh_block = DBLQH;
    m_ldm_instance_used = this;
  }
  else
  {
    m_lqh_block = DBQLQH;
    m_is_query_block = true;
    m_is_in_query_thread = true;
    m_ldm_instance_used = nullptr;
    ndbrequire(blockNo == DBQACC);
    addRecSignal(GSN_STTOR, &Dbacc::execSTTOR);
#ifdef ACC_OLD
    addRecSignal(GSN_EXPANDCHECK2, &Dbacc::execEXPANDCHECK2);
    addRecSignal(GSN_SHRINKCHECK2, &Dbacc::execSHRINKCHECK2);
#endif//ACC_OLD
    addRecSignal(GSN_ACCSEIZEREQ, &Dbacc::execACCSEIZEREQ);
    addRecSignal(GSN_READ_CONFIG_REQ, &Dbacc::execREAD_CONFIG_REQ, true);
    addRecSignal(GSN_CONTINUEB, &Dbacc::execCONTINUEB);
    addRecSignal(GSN_DUMP_STATE_ORD, &Dbacc::execDUMP_STATE_ORD);
  }
  initData();

  c_transient_pools[DBACC_OPERATION_RECORD_TRANSIENT_POOL_INDEX] =
    &oprec_pool;
  static_assert(c_transient_pool_count == 1);
  c_transient_pools_shrinking.clear();
}//Dbacc::Dbacc()

Dbacc::~Dbacc() 
{
  deallocRecord((void **)&tabrec, "Tabrec",
		sizeof(Tabrec),
		ctablesize);
}//Dbacc::~Dbacc()
BLOCK_FUNCTIONS(Dbacc)
