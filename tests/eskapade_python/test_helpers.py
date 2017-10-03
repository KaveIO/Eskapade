import unittest
import unittest.mock as mock

from eskapade.helpers import apply_transform_funcs, process_transform_funcs


class TransformFuncsTest(unittest.TestCase):
    """Test for transformation functions"""

    maxDiff = None

    @mock.patch('eskapade.helpers.type')
    @mock.patch('eskapade.helpers.process_transform_funcs')
    def test_apply_transform_funcs(self, mock_process, mock_type):
        """Test applying transformation functions"""

        # create mock transformation functions
        funcs = ['func0', mock.Mock(name='func1')]
        trans_objs = [mock.Mock(name='transformed_obj{:d}'.format(it)) for it in range(2)]
        funcs[1].return_value = trans_objs[1]

        # create mock transformation object and its type
        obj_cls = type('obj_cls', (), {'func0': mock.Mock(name='func0'), 'no_func': type('non_callable', (), {})()})
        obj_cls.func0.return_value = trans_objs[0]
        obj = mock.Mock(name='obj')
        obj.__class__ = obj_cls
        mock_type.side_effect = lambda a: obj_cls if a is obj else type(a)

        # create mock function arguments
        func_args = dict((fun, tuple(mock.Mock(name='arg{:d}_{:d}'.format(fi, ai)) for ai in range(2)))
                         for fi, fun in enumerate(funcs))
        func_kwargs = dict((fun, dict(('key{:d}'.format(ai), mock.Mock(name='kwarg{:d}_{:d}'.format(fi, ai)))
                                      for ai in range(2))) for fi, fun in enumerate(funcs))

        # make sure functions and arguments are processed as expected
        mock_process.side_effect = lambda funcs, args, kwargs: [(f, args[f], kwargs[f]) for f in funcs]

        # test normal operation
        ret_obj = apply_transform_funcs(obj, funcs, func_args, func_kwargs)
        self.assertIs(ret_obj, trans_objs[-1], 'unexpected transformed object returned')
        obj_cls.func0.assert_called_once_with(obj, *func_args[funcs[0]], **func_kwargs[funcs[0]])
        funcs[1].assert_called_once_with(trans_objs[0], *func_args[funcs[1]], **func_kwargs[funcs[1]])

        # test with non-existing member function
        with self.assertRaises(AttributeError):
            apply_transform_funcs(obj, ['foo'], dict(foo=()), dict(foo={}))

        # test with non-callable member
        with self.assertRaises(TypeError):
            apply_transform_funcs(obj, ['no_func'], dict(no_func=()), dict(no_func={}))

    def test_process_transform_funcs(self):
        """Test processing transformation functions"""

        # create mock functions and arguments
        funcs = ['func0', 'func1', 'func2', mock.Mock(name='func3'),
                 ('func4', tuple(mock.Mock(name='arg4_{:d}'.format(it)) for it in range(2)),
                  dict(('key{:d}'.format(it), mock.Mock(name='kwarg4_{:d}'.format(it))) for it in range(2)))]
        func_args = {'func1': (), 'func2': tuple(mock.Mock(name='arg2_{:d}'.format(it)) for it in range(2)),
                     funcs[3]: tuple(mock.Mock(name='arg3_{:d}'.format(it)) for it in range(2))}
        func_kwargs = {'func1': {}, 'func2': {},
                       funcs[3]: dict(('key{:d}'.format(it), mock.Mock(name='kwarg3_{:d}'.format(it)))
                                      for it in range(2))}

        # expected returned values
        funcs_normal = [f if isinstance(f, tuple) else (f, tuple(func_args.get(f, ())), dict(func_kwargs.get(f, {})))
                        for f in funcs]
        funcs_no_args = [f if isinstance(f, tuple) else (f, (), {}) for f in funcs]

        # test normal operation (1)
        ret_funcs = process_transform_funcs(funcs, func_args, func_kwargs)
        self.assertListEqual(ret_funcs, funcs_normal, 'unexpected list of functions for normal operation (1)')

        # test normal operation (2)
        ret_funcs = process_transform_funcs(trans_funcs=funcs, func_args=func_args, func_kwargs=func_kwargs)
        self.assertListEqual(ret_funcs, funcs_normal, 'unexpected list of functions for normal operation (2)')

        # test operation without explicit arguments
        ret_funcs = process_transform_funcs(funcs)
        self.assertListEqual(ret_funcs, funcs_no_args, 'unexpected list of functions without explicit arguments')

        # test specifying incorrect function format
        with self.assertRaises(ValueError):
            process_transform_funcs([('foo',)])

        # test specifying incorrect arguments
        with self.assertRaises(TypeError):
            process_transform_funcs([('foo', mock.Mock(name='args'), {})])
        with self.assertRaises(TypeError):
            process_transform_funcs(['foo'], func_args=dict(foo=mock.Mock(name='args')))
        with self.assertRaises(ValueError):
            process_transform_funcs(['foo'], func_args=dict(bar=mock.Mock(name='args')))
        with self.assertRaises(RuntimeError):
            process_transform_funcs([('foo', mock.Mock(name='args1'), {})],
                                    func_args=dict(foo=mock.Mock(name='args2')))

        # test specifying incorrect keyword arguments
        with self.assertRaises(TypeError):
            process_transform_funcs([('foo', (), mock.Mock(name='kwargs'))])
        with self.assertRaises(TypeError):
            process_transform_funcs(['foo'], func_kwargs=dict(foo=mock.Mock(name='kwargs')))
        with self.assertRaises(ValueError):
            process_transform_funcs(['foo'], func_kwargs=dict(bar=mock.Mock(name='kwargs')))
        with self.assertRaises(RuntimeError):
            process_transform_funcs([('foo', (), mock.Mock(name='kwargs1'))],
                                    func_kwargs=dict(foo=mock.Mock(name='kwargs2')))

        # test with non-callable "function"
        with self.assertRaises(TypeError):
            process_transform_funcs([type('non_callable', (), {})()])
