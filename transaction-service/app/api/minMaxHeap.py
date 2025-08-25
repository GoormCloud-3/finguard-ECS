class MinHeap:
    def __init__(self, arr=None):
        self.heap = []
        if arr:
            for v in arr:
                self.push(v)
    def push(self, val):
        self.heap.append(val)
        self._bubble_up()
    def pop(self):
        if self.size() == 0:
            return None
        top = self.heap[0]
        end = self.heap.pop()
        if self.size() > 0:
            self.heap[0] = end
            self._sink_down()
        return top
    def peek(self):
        return self.heap[0] if self.heap else None
    def size(self):
        return len(self.heap)
    def to_array(self):
        return list(self.heap)
    def _bubble_up(self):
        i = len(self.heap) - 1
        val = self.heap[i]
        while i > 0:
            parent = (i - 1) // 2
            if self.heap[parent] <= val:
                break
            self.heap[i] = self.heap[parent]
            i = parent
        self.heap[i] = val
    def _sink_down(self):
        i = 0
        length = len(self.heap)
        val = self.heap[0]
        while True:
            left = 2 * i + 1
            right = 2 * i + 2
            swap = None
            if left < length and self.heap[left] < val:
                swap = left
            if right < length and self.heap[right] < (self.heap[left] if swap is not None else val):
                swap = right
            if swap is None:
                break
            self.heap[i] = self.heap[swap]
            i = swap
        self.heap[i] = val

class MaxHeap:
    def __init__(self, arr=None):
        if arr:
            inv = [-v for v in arr]
        else:
            inv = []
        self.heap = MinHeap(inv)
    def push(self, val):
        self.heap.push(-val)
    def pop(self):
        v = self.heap.pop()
        return -v if v is not None else None
    def peek(self):
        v = self.heap.peek()
        return -v if v is not None else None
    def size(self):
        return self.heap.size()
    def to_array(self):
        return [-v for v in self.heap.to_array()]
