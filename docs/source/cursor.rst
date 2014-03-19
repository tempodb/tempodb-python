Cursors
=======

The :mod:`tempodb.protocol.cursor` module contaions functionality for working 
with database cursors.  Each :class:`Cursor` object is a standard Python 
iterable that represents a one-time use array of data from the TempoDB API 
(i.e. after you have iterated through a cursor, you must make another API call 
to read the data again)::

  >>> data = [d for d in response.data]
  >>> data
  [DataPoint, DataPoint, ...]
  >>> data2 = [d for d in response.data]
  >>> data2
  []

.. automodule:: tempodb.protocol.cursor
   :members:
