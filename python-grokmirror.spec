%if 0%{?fedora} > 12
%global with_python3 0
%else
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%endif

Name:           python-grokmirror
Version:        0.4.0
Release:        1%{?dist}
Summary:        Framework to smartly mirror git repositories

License:        GPLv3+
URL:            https://git.kernel.org/cgit/utils/grokmirror/grokmirror.git
Source0:        https://www.kernel.org/pub/software/network/grokmirror/grokmirror-%{version}.tar.xz

BuildArch:      noarch
BuildRequires:  python2-devel, python-setuptools
Requires:       GitPython, python-anyjson, python-setuptools

%description
Grokmirror was written to make mirroring large git repository
collections more efficient. Grokmirror uses the manifest file published
by the master mirror in order to figure out which repositories to
clone, and to track which repositories require updating. The process is
extremely lightweight and efficient both for the master and for the
mirrors.

%prep
%setup -q -n grokmirror-%{version}


%build
%{__python} setup.py build


%install
rm -rf %{buildroot}
%{__python} setup.py install -O1 --skip-build --root %{buildroot}

%{__mkdir_p} -m 0755 \
    %{buildroot}%{_bindir} \
    %{buildroot}%{_mandir}/man1

%{__install} -m 0644 man/*.1 %{buildroot}/%{_mandir}/man1/


%files
%doc README.rst COPYING repos.conf fsck.conf
%{python_sitelib}/grokmirror/
%{python_sitelib}/*.egg-info
%{_bindir}/grok-*
%{_mandir}/*/*


%changelog
* Sat Apr 11 2015 Konstantin Ryabitsev <konstantin@linuxfoundation.org> - 0.4.0-1
- Relase final 0.4.0

* Mon Apr 14 2014 Konstantin Ryabitsev <mricon@kernel.org> - 0.4.0-0.pre.13
- Update to 0.4.0-pre with new features

* Fri Aug 16 2013 Konstantin Ryabitsev <mricon@kernel.org> - 0.3.5-1
- Update to 0.3.5 containing minor feature enhancements

* Fri Jun 14 2013 Konstantin Ryabitsev <mricon@kernel.org> - 0.3.4-1
- Update to 0.3.4 containing minor bugfixes

* Mon May 27 2013 Konstantin Ryabitsev <mricon@kernel.org> - 0.3.3-1
- Update to 0.3.3 containing bugfixes and new features

* Mon May 13 2013 Konstantin Ryabitsev <mricon@kernel.org> - 0.3.2-1
- Update to 0.3.2 containing important bugfixes and minor new features

* Mon May 13 2013 Konstantin Ryabitsev <mricon@kernel.org> - 0.3.1-1
- Update to 0.3.1 containing important bugfixes

* Mon May 06 2013 Konstantin Ryabitsev <mricon@kernel.org> - 0.3-1
- Preparing for 0.3 with new features.

* Thu Apr 25 2013 Konstantin Ryabitsev <mricon@kernel.org> - 0.2-1
- Version 0.2 with new features and manpages.

* Wed Apr 03 2013 Konstantin Ryabitsev <mricon@kernel.org> - 0.1-1
- Initial packaging
