import sys

from pytest import raises

from eskapade import entry_points
from eskapade.entry_points import eskapade_bootstrap


def test_bootstrap_args():
    """Test eskapade_bootstrap arguments handling."""
    # no required arguments
    with raises(SystemExit) as excinfo:
        sys.argv[1:] = []
        eskapade_bootstrap()
        assert 'the following arguments are required' in str(excinfo.value)
    # unrecognized arguments
    with raises(SystemExit) as excinfo:
        sys.argv[1:] = ['package', 'unrecognized_arg']
        eskapade_bootstrap()
        assert 'unrecognized arguments' in str(excinfo.value)
    # use of Eskapade reserved name
    with raises(AttributeError, match='eskapade is reserved by Eskapade'):
        sys.argv[1:] = ['eskapade']
        eskapade_bootstrap()
    # use of Eskapade reserved name
    with raises(AttributeError, match='Link is reserved by Eskapade'):
        sys.argv[1:] = ['package', '-l', 'Link']
        eskapade_bootstrap()
    # missed project root dir
    with raises(AttributeError, match='--project_root_dir'):
        sys.argv[1:] = ['package', '--project_root_dir', '']
        eskapade_bootstrap()


def assert_structure(local_path, package_name, link_name, macro_name, notebook_name):
    """Check the structure of the project generated by eskapade_bootstrap."""
    assert local_path.join('setup.py').check()
    package_dir = local_path.join(package_name)
    assert package_dir.check()

    expected_package_paths = ['links', 'links/__init__.py', 'links/{}.py'.format(link_name.lower()),
                              '__init__.py', '{}.py'.format(macro_name), '{}.ipynb'.format(notebook_name)]
    package_paths = [package_dir.bestrelpath(path) for path in package_dir.visit()]
    assert len(set(expected_package_paths).difference(set(package_paths))) == 0


def test_bootstrap_structure_default(tmpdir):
    """Test eskapade_bootstrap with the default argument values."""
    package_name = 'package'
    sys.argv[1:] = [package_name, '--project_root_dir', "{}".format(tmpdir.realpath())]
    eskapade_bootstrap()
    assert_structure(tmpdir, package_name, entry_points.DEFAULT_LINK_NAME, entry_points.DEFAULT_MACRO_NAME,
                     entry_points.DEFAULT_NOTEBOOK_NAME)


def test_bootstrap_structure(tmpdir):
    """Test eskapade_bootstrap with all given arguments."""
    package_name = 'package'
    link_name = 'TestLink'
    macro_name = 'testmacro'
    notebook_name = 'testnotebook'
    sys.argv[1:] = [package_name, '--project_root_dir', "{}".format(tmpdir.realpath()),
                    '-l', link_name, '-m', macro_name, '-n', notebook_name]
    eskapade_bootstrap()
    assert_structure(tmpdir, package_name, link_name, macro_name, notebook_name)