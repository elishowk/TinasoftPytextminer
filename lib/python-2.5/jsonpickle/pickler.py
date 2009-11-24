# -*- coding: utf-8 -*-
#
# Copyright (C) 2008 John Paulett (john -at- 7oars.com)
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
import types
import jsonpickle.util as util
import jsonpickle.tags as tags


class Pickler(object):
    """Converts a Python object to a JSON representation.
    
    Setting unpicklable to False removes the ability to regenerate
    the objects into object types beyond what the standard simplejson
    library supports.

    Setting max_depth to a negative number means there is no
    limit to the depth jsonpickle should recurse into an
    object.  Setting it to zero or higher places a hard limit
    on how deep jsonpickle recurses into objects, dictionaries, etc.

    >>> p = Pickler()
    >>> p.flatten('hello world')
    'hello world'
    """
    
    def __init__(self, unpicklable=True, max_depth=None):
        self.unpicklable = unpicklable
        ## The current recursion depth
        self._depth = -1
        ## The maximal recursion depth
        self._max_depth = max_depth
        ## Maps id(obj) to reference names
        self._objs = {}
        ## The namestack grows whenever we recurse into a child object
        self._namestack = []

    def _reset(self):
        self._objs = {}
        self._namestack = []

    def _push(self):
        """Steps down one level in the namespace.
        """
        self._depth += 1

    def _pop(self, value):
        """Step up one level in the namespace and return the value.
        If we're at the root, reset the pickler's state.
        """
        self._depth -= 1
        if self._depth == -1:
            self._reset()
        return value

    def _mkref(self, obj):
        objid = id(obj)
        if objid not in self._objs:
            self._objs[objid] = '/' + '/'.join(self._namestack)
            return True
        return False

    def _getref(self, obj):
        return {tags.REF: self._objs.get(id(obj))}

    def flatten(self, obj):
        """Takes an object and returns a JSON-safe representation of it.
        
        Simply returns any of the basic builtin datatypes
        
        >>> p = Pickler()
        >>> p.flatten('hello world')
        'hello world'
        >>> p.flatten(u'hello world')
        u'hello world'
        >>> p.flatten(49)
        49
        >>> p.flatten(350.0)
        350.0
        >>> p.flatten(True)
        True
        >>> p.flatten(False)
        False
        >>> r = p.flatten(None)
        >>> r is None
        True
        >>> p.flatten(False)
        False
        >>> p.flatten([1, 2, 3, 4])
        [1, 2, 3, 4]
        >>> p.flatten((1,2,))[tags.TUPLE]
        [1, 2]
        >>> p.flatten({'key': 'value'})
        {'key': 'value'}
        """

        self._push()
        
        if self._depth == self._max_depth:
            return self._pop(repr(obj))

        if util.is_primitive(obj):
            return self._pop(obj)

        if util.is_list(obj):
            return self._pop([ self.flatten(v) for v in obj ])

        # We handle tuples and sets by encoding them in a "(tuple|set)dict"
        if util.is_tuple(obj):
            return self._pop({tags.TUPLE: [ self.flatten(v) for v in obj ]})

        if util.is_set(obj):
            return self._pop({tags.SET: [ self.flatten(v) for v in obj ]})

        if util.is_dictionary(obj):
            return self._pop(self._flatten_dict_obj(obj, obj.__class__()))

        if util.is_type(obj):
            return self._pop(_mktyperef(obj))

        if util.is_object(obj):
            data = {}
            has_class = hasattr(obj, '__class__')
            has_dict = hasattr(obj, '__dict__')
            if self._mkref(obj):
                if has_class and not util.is_repr(obj):
                    module, name = _getclassdetail(obj)
                    if self.unpicklable is True:
                        data[tags.OBJECT] = '%s.%s' % (module, name)

                if util.is_repr(obj):
                    if self.unpicklable is True:
                        data[tags.REPR] = '%s/%s' % (obj.__class__.__module__,
                                                     repr(obj))
                    else:
                        data = unicode(obj)
                    return self._pop(data)

                if util.is_dictionary_subclass(obj):
                    return self._pop(self._flatten_dict_obj(obj, data))

                if util.is_noncomplex(obj):
                    return self._pop([self.flatten(v) for v in obj])

                if has_dict:
                    return self._pop(self._flatten_dict_obj(obj.__dict__, data))
            else:
                # We've seen this object before so place an object
                # reference tag in the data. This avoids infinite recursion
                # when processing cyclical objects.
                return self._pop(self._getref(obj))

            return self._pop(data)
        # else, what else? (methods, functions, old style classes...)

    def _flatten_dict_obj(self, obj, data):
        """_flatten_dict_obj recursively calls to flatten() on a dictionary's values.
        and places them into data.
        """
        for k, v in obj.iteritems():
            if util.is_function(v):
                continue
            if type(k) not in types.StringTypes:
                k = repr(k)
            self._namestack.append(k)
            data[k] = self.flatten(v)
            self._namestack.pop()
        return data

def _mktyperef(obj):
    """Returns a typeref dictionary.  This is used to implement referencing.

    >>> from jsonpickle import tags
    >>> _mktyperef(AssertionError)[tags.TYPE].rsplit('.', 1)[0]
    'exceptions'

    >>> _mktyperef(AssertionError)[tags.TYPE].rsplit('.', 1)[-1]
    'AssertionError'
    """
    return {tags.TYPE: '%s.%s' % (obj.__module__, obj.__name__)}

def _getclassdetail(obj):
    """Helper class to return the class of an object.
    
    >>> class Klass(object): pass
    >>> _getclassdetail(Klass())
    ('jsonpickle.pickler', 'Klass')
    >>> _getclassdetail(25)
    ('__builtin__', 'int')
    >>> _getclassdetail(None)
    ('__builtin__', 'NoneType')
    >>> _getclassdetail(False)
    ('__builtin__', 'bool')
    """
    cls = obj.__class__
    module = getattr(cls, '__module__')
    name = getattr(cls, '__name__')
    return module, name
