#ifndef JETS_RDF_R_RESOURCES_H
#define JETS_RDF_R_RESOURCES_H

#include <string>

#include "../rdf/rdf_err.h"
#include "../rdf/rdf_ast.h"

// Component to manage all the rdf resources and literals of a graph
namespace jets::rdf {
class RManager;
using RManagerPtr = std::shared_ptr<RManager>;

/////////////////////////////////////////////////////////////////////////////////////////
// JetsResources is a cache of resources for rete exper
struct JetsResources {

  void
  initialize(RManager * rmgr);

  inline bool
  is_initialized()const
  {
    if(not this->jets__entity_property) return false;
    return true;
  }

  r_index jets__entity_property{nullptr};
  r_index jets__value_property{nullptr};
  r_index jets__key{nullptr};

};

} // namespace jets::rdf
#endif // JETS_RDF_R_RESOURCES_H