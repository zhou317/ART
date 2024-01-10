#include <iostream>

#include "../src/art/art.h"

int main() {
  art::ArtTree<int64_t> artTree;

  artTree.set("ant", 1);
  artTree.set("and", 2);
  artTree.set("any", 3);
  artTree.set("are", 4);
  artTree.set("art", 5);

  std::cout << artTree << std::endl;
}
