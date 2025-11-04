package index

import (
	"bytes"
	"encoding/binary"
)

const (
	BNODE_NODE         = 1
	BNODE_LEAF         = 2
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

type BNode struct {
	data []byte
}

type BTree struct {
	//A non-zero page number
	root uint64

	get func(uint64) BNode //Dereference a pointer
	new func(BNode) uint64 //Allocated a new page
	del func(uint64)       //Deallocate a page

}

// Initiates a page
func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
}

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

func offsetPos(node BNode, idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// Node Lookup
func nodeLookUpLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	//First key, offset 0, is a copy from the parent node. Hence less or equal to the key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

//

func treeInsert(tree *BTree, node BNode, key, value []byte) BNode {

	newBNode := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	idx := nodeLookUpLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafInsert(newBNode, node, idx, key, value)
		} else {
			leafInsert(newBNode, node, idx+1, key, value)
		}
	case BNODE_NODE:
		n

	}

}

func nodeInsert(tree *BTree, new, node BNode, idx uint16, key, value []byte) {
	//Insert Nodee
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)

	knode = treeInsert(tree, knode, key, value)

	nsplit, splited := nodeSplit3(knode)

	replaceChildNode(tree, new, node, idx, splited[:nsplit]...)

}

func nodeSplit2(left, right, old BNode) {

}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		//TODO: Why not return the old Node as it was.
		// In both scenario we are returning copies of the same
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}

	nodeSplit2(left, right, old)

	//Basically resizing the left tree to a normal page size
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	//Left Node is Larga
	leftOfLeft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftOfLeft, middle, left)
	return 3, [3]BNode{leftOfLeft, middle, left}
}

func replaceChildNode(tree *BTree, new, old BNode, idx uint16, childNodes ...BNode) {
	inc := uint16(len(childNodes))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range childNodes {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx-1))

}

func leafInsert(new BNode, old BNode, idx uint16, key, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func nodeAppendRange(new BNode, old BNode, dstNew, srcOld, n uint16) {
	if n == 0 {
		return
	}
	//Append Pointers
	for i := uint16(1); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	//Offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])

}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	//ptrs
	new.setPtr(idx, ptr)

	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))

	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key))+uint16(len(val)))
}

func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx-1))
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)

	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{}
	}
	tree.del(kptr)

	newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	//Check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)

	switch {
	case mergeDir < 0:
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		//replaceChildNode(newNode,node,idx,tree.new(merged),merged.getKey(0))
	case mergeDir > 0:
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx - 1))
		//replaceChildNode(newNode,node,idx,tree.new(merged),merged.getKey(0))
	case mergeDir == 0:
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		//replaceChildNode(newNode,node,idx,tree.new(merged),merged.getKey(0))

	}
	return newNode
}

func nodeMerge(new, left, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())

}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nkeys() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return 1, sibling
		}

	}
	return 0, BNode{}

}

func mergeNodes(newNode, left, right BNode) {
	newNode.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(newNode, left, 0, 0, left.nkeys())
	nodeAppendRange(newNode, right, left.nkeys(), 0, right.nkeys())

}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	//Find the key
	idx := nodeLookUpLE(node, key)

	//act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		//delete key in the leaf
		newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(newNode, node, idx)
		return newNode
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad mode")
	}
}

func (tree *BTree) Delete(key []byte) bool {
	//Verify that the key is not an empty array
	//Verify the size of the key does not surpass the BTREE_MAX_KEY_SIZE
	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false //not found
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true

}

func (tree *BTree) Insert(key, value []byte) {
	if tree.root == 0 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, value)
		tree.root = tree.new(root)
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)

	node = treeInsert(tree, node, key, value)
	nsplit, splitted := nodeSplit3(node)

	if nsplit > 1 {
		//the root was split and a new level
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	}

}
