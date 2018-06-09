import math
import random


class BPlusTreeError(Exception):
    pass


class Node():

    def __init__(self):
        self.keys = []
        self.children = []

    def is_empty(self):
        return len(self.keys) == 0

    def is_leaf(self):
        return len(self.keys) == 0 or not isinstance(self.children[0], Node)

    def is_full(self, order):
        return len(self.keys) >= order

    def is_poor(self, order):
        return len(self.keys) < math.ceil(order / 2)

    def insert_idx(self, key):
        if self.is_empty():
            return 0

        if key < self.keys[0]:
            return 0

        size = len(self.keys)
        if key >= self.keys[-1]:
            return size

        if size >= 2:
            for i in range(size - 1):
                if self.keys[i] <= key and key < self.keys[i + 1]:
                    return i + 1

        raise Exception('logical error')


ROOT = 0
INTERNAL = 1
LEAF = 2


class BPlusTree():

    def __init__(self, order):
        self.root = Node()
        self.order = order
        # self.duplicate = duplicate

    def search(self, key):
        node = self._tree_search(key, self.root)
        # if not self.duplicate:
        for i, k in enumerate(node.keys):
            if k == key:
                return node.children[i]
        return None

    def insert(self, key, value):
        leaf_node = self._tree_search(key, self.root)

        if not leaf_node.is_empty():
            for i, k in enumerate(leaf_node.keys):
                if k == key:
                    child = leaf_node.children[i]
                    if isinstance(child, list):
                        leaf_node.children[i].append(value)
                    else:
                        leaf_node.children[i] = [child, value]
                    return

        idx = leaf_node.insert_idx(key)
        leaf_node.keys.insert(idx, key)
        leaf_node.children.insert(idx, value)

        # not full
        if not leaf_node.is_full(self.order):
            return
        else:
            key, left, right = self._split_node(leaf_node)
            self._insert_in_parent(key, left, right)

    def delete(self, key, value=None):
        node = self._tree_search(key, self.root)
        self._delete_entry(node, key, value)

    def _delete_entry(self, node, key, value):
        try:
            idx = node.keys.index(key)
            if isinstance(value, Node):
                node.keys.pop(idx)
                node.children.remove(value)
            elif value is not None and isinstance(node.children[idx], list) and len(node.children[idx]) > 1:
                node.children[idx].remove(value)
            else:
                node.keys.pop(idx)
                node.children.pop(idx)
        except ValueError as e:
            raise BPlusTreeError('{} is not in tree'.format(key))

        if not node.is_poor:
            return

        if node is self.root:
            self.root = node.children[0]
        else:
            parent = self._find_parent(node)
            left, right, key = self._find_siblings(parent, node)

            # combine if fit in one single node
            if len(left.keys) + len(right.keys) < self.order:
                if not right.is_leaf():
                    left.keys += [key] + right.keys
                    left.children += right.children
                else:
                    left.keys += right.keys
                    left.children = left.children[:-1] + right.children
                self._delete_entry(parent, key, right)

            # borrow
            else:
                # right == node
                if right is node:
                    if not right.is_leaf():
                        right.keys = [key] + right.keys
                        right.children = [left.children[-1]] + right.children
                        self._replace_key(parent, key, left.keys[-1])
                        left.keys.pop(-1)
                        left.children.pop(-1)
                    else:
                        right.keys = [left.keys[-1]] + right.keys
                        right.children = [left.children[-2]] + right.children
                        self._replace_key(parent, key, right.keys[0])
                        left.keys.pop(-1)
                        left.children.pop(-2)

                # left == node
                elif left is node:
                    if not left.is_leaf():
                        left.keys += [key]
                        left.children += [right.children[0]]
                        self._replace_key(parent, key, right.keys[0])
                        right.keys.pop(0)
                        right.children.pop(0)
                    else:
                        left.keys += [right.keys[0]]
                        left.children.insert(-1, right.children[0])
                        self._replace_key(parent, key, right.keys[1])
                        right.keys.pop(0)
                        right.children.pop(0)

    def _find_siblings(self, parent, node):
        idx = parent.children.index(node)
        if idx - 1 >= 0:
            key = parent.keys[idx - 1]
            left = parent.children[idx - 1]
            return left, node, key
        elif idx + 1 < len(parent.children):
            key = parent.keys[idx]
            right = parent.children[idx + 1]
            return node, right, key

        raise BPlusTreeError('sibling not found')

    def _replace_key(self, node, key, new_key):
        for i, k in enumerate(node.keys):
            if k == key:
                node.keys[i] = new_key
                break

    def _insert_in_parent(self, key, left, right):
        parent = self._find_parent(left)

        # left node is root
        if parent is None:
            root = Node()
            root.keys.append(key)
            root.children += [left, right]
            self.root = root
        else:
            idx = parent.insert_idx(key)
            parent.keys.insert(idx, key)
            parent.children.insert(idx + 1, right)

            if not parent.is_full(self.order):
                return
            else:
                key, left, right = self._split_node(parent)
                self._insert_in_parent(key, left, right)

    def _split_node(self, node):
        half = math.floor(self.order / 2)
        new_node = Node()
        if node.is_leaf():
            new_node.keys = node.keys[half:]
            new_node.children = node.children[half:]
            key = new_node.keys[0]
            node.keys = node.keys[:half]
            node.children = node.children[:half] + [new_node]  # connect between siblings
        else:
            new_node.keys = node.keys[half + 1:]
            new_node.children = node.children[half + 1:]
            key = node.keys[half]
            node.keys = node.keys[:half]
            node.children = node.children[:half + 1]

        return key, node, new_node

    def _find_parent(self, node):
        key = node.keys[-1]  # max key

        if node is self.root:
            return
        else:
            parent = self.root
            while True:
                # not found parent
                if not isinstance(parent, Node):
                    return None

                for i, k in enumerate(parent.keys):
                    if key <= k:
                        break
                if key >= k:
                    if parent.children[i + 1] is node:
                        return parent
                    else:
                        parent = parent.children[i + 1]
                else:
                    if parent.children[i] is node:
                        return parent
                    else:
                        parent = parent.children[i]

    def _tree_search(self, key, node):
        # print(node.keys)
        # if node.type == LEAF or len(node.children) == 0 or not isinstance(node.children[0], Node):
        if node.is_leaf():
            return node
        else:
            for i, k in enumerate(node.keys):
                if k is not None and key < k:
                    break

            if key < k:
                return self._tree_search(key, node.children[i])
            else:
                return self._tree_search(key, node.children[i + 1])

    def print_all(self, key=True):
        node = self.root
        while isinstance(node.children[0], Node):
            node = node.children[0]

        while node is not None:
            if key:
                for k in node.keys:
                    print(k, end=' ')
            else:
                for v in node.children:
                    if not isinstance(v, Node):
                        print(v, end=' ')
            if isinstance(node.children[-1], Node):
                node = node.children[-1]
            else:
                node = None


if __name__ == '__main__':
    tree = BPlusTree(10)
    a = list(range(1001))
    for i in range(20):
        key = random.randint(0, 1000)
        # print('insert {}'.format(key))
        # print(tree.root.keys)
        tree.insert(i, a[i])
        tree.insert(i, a[i + 1])

    tree.delete(2, 3)

    # print(tree.search(66))
    tree.print_all(False)
