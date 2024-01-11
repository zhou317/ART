#pragma once

#ifdef ART_MACRO_OPTIMISTIC_LOCK

#define ART_MACRO_SET_LOCK_BIT(v) ((v) + 2)
#define ART_MACRO_IS_OBSOLETE(v) ((v)&1)
#define ART_MACRO_IS_LOCKED(v) ((v)&2)

#define ART_MACRO_READ_LOCK_OR_RESTART(node, version_varname, restart_label) \
  version_varname = (node)->version.load();                                  \
  while (ART_MACRO_IS_LOCKED(version_varname)) {                             \
    __builtin_ia32_pause();                                                  \
    version_varname = (node)->version.load();                                \
  }                                                                          \
  if (ART_MACRO_IS_OBSOLETE(version_varname)) goto restart_label

#define ART_MACRO_READ_UNLOCK_OR_RESTART(node, version_varname, restart_label) \
  if ((version_varname) != (node)->version.load()) goto restart_label

#define ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(node, version_varname, \
                                              restart_label)         \
  if (!(node)->version.compare_exchange_strong(                      \
          version_varname, ART_MACRO_SET_LOCK_BIT(version_varname))) \
  goto restart_label

#define ART_MACRO_WRITE_UNLOCK(node) (node)->version.fetch_add(2)

#define ART_MACRO_WRITE_UNLOCK_OBSOLETE(node) (node)->version.fetch_add(3)

#define ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(               \
    node, version_varname, lockNode, restart_label)                      \
  do {                                                                   \
    if (!(node)->version.compare_exchange_strong(                        \
            version_varname, ART_MACRO_SET_LOCK_BIT(version_varname))) { \
      ART_MACRO_WRITE_UNLOCK(lockNode);                                  \
      goto restart_label;                                                \
    }                                                                    \
  } while (0)

#define ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE_TWO(           \
    node, version_varname, lockNode_p, lockNode_pp, restart_label)       \
  do {                                                                   \
    if (!(node)->version.compare_exchange_strong(                        \
            version_varname, ART_MACRO_SET_LOCK_BIT(version_varname))) { \
      ART_MACRO_WRITE_UNLOCK(lockNode_p);                                \
      ART_MACRO_WRITE_UNLOCK(lockNode_pp);                               \
      goto restart_label;                                                \
    }                                                                    \
  } while (0)

#else

#define ART_MACRO_READ_LOCK_OR_RESTART(node, version_varname, restart_label) \
  (node)->rw_mutex.RLock()

#define ART_MACRO_READ_UNLOCK_OR_RESTART(node, version_varname, restart_label) \
  (node)->rw_mutex.RUnlock()

#define ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(node, version_varname, \
                                              restart_label)         \
  (node)->rw_mutex.RUnlock()

#define ART_MACRO_WRITE_UNLOCK(node) (node)->version.fetch_add(2)

#define ART_MACRO_WRITE_UNLOCK_OBSOLETE(node) (node)->version.fetch_add(3)

#define ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(               \
    node, version_varname, lockNode, restart_label)                      \
  do {                                                                   \
    if (!(node)->version.compare_exchange_strong(                        \
            version_varname, ART_MACRO_SET_LOCK_BIT(version_varname))) { \
      ART_MACRO_WRITE_UNLOCK(lockNode);                                  \
      goto restart_label;                                                \
    }                                                                    \
  } while (0)

#define ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE_TWO(           \
    node, version_varname, lockNode_p, lockNode_pp, restart_label)       \
  do {                                                                   \
    if (!(node)->version.compare_exchange_strong(                        \
            version_varname, ART_MACRO_SET_LOCK_BIT(version_varname))) { \
      ART_MACRO_WRITE_UNLOCK(lockNode_p);                                \
      ART_MACRO_WRITE_UNLOCK(lockNode_pp);                               \
      goto restart_label;                                                \
    }                                                                    \
  } while (0)

#endif
