%global srcname   grokmirror
%global groupname mirror
%global username  mirror
%global userhome  %{_sharedstatedir}/grokmirror

Name:           python-%{srcname}
Version:        2.0.1
Release:        1%{?dist}
Summary:        Framework to smartly mirror git repositories

License:        GPLv3+
URL:            https://git.kernel.org/pub/scm/utils/grokmirror/grokmirror.git
Source0:        https://www.kernel.org/pub/software/network/grokmirror/grokmirror-%{version}.tar.xz

BuildArch:      noarch

%global _description %{expand:
Grokmirror was written to make mirroring large git repository
collections more efficient. Grokmirror uses the manifest file published
by the master mirror in order to figure out which repositories to
clone, and to track which repositories require updating. The process is
extremely lightweight and efficient both for the master and for the
mirrors.}

%description %_description

%package -n python3-%{srcname}
Summary:       %{summary}
Requires(pre): shadow-utils
Requires:      git-core, python3-packaging, python3-requests
BuildRequires: python3-devel, python3-setuptools
BuildRequires: systemd
Obsoletes:     python-%{srcname} < 2, python2-%{srcname} < 2

%description -n python3-%{srcname} %_description

%prep
%autosetup -n %{srcname}-%{version}

%build
%py3_build

%install
%py3_install

%{__mkdir_p} -m 0755 \
    %{buildroot}%{userhome} \
    %{buildroot}%{_sysconfdir}/%{srcname} \
    %{buildroot}%{_sysconfdir}/logrotate.d \
    %{buildroot}%{_unitdir} \
    %{buildroot}%{_bindir} \
    %{buildroot}%{_tmpfilesdir} \
    %{buildroot}%{_mandir}/man1 \
    %{buildroot}%{_localstatedir}/log/%{srcname} \
    %{buildroot}/run/%{srcname}

%{__install} -m 0644 man/*.1 %{buildroot}/%{_mandir}/man1/
%{__install} -m 0644 contrib/*.service %{buildroot}/%{_unitdir}/
%{__install} -m 0644 contrib/*.timer %{buildroot}/%{_unitdir}/
%{__install} -m 0644 contrib/logrotate %{buildroot}/%{_sysconfdir}/logrotate.d/grokmirror
%{__install} -m 0644 grokmirror.conf %{buildroot}/%{_sysconfdir}/%{srcname}/grokmirror.conf.example

echo "d /run/%{srcname} 0755 %{username} %{groupname}" > %{buildroot}/%{_tmpfilesdir}/%{srcname}.conf

%pre -n python3-%{srcname}
getent group %{groupname} >/dev/null || groupadd -r %{groupname}
getent passwd %{username} >/dev/null || \
    useradd -r -g %{groupname} -d %{userhome} -s /sbin/nologin \
    -c "Grokmirror user" %{username}
exit 0

%files -n python3-%{srcname}
%license LICENSE.txt
%doc README.rst grokmirror.conf
%dir %attr(0750, %{username}, %{groupname}) %{userhome}
%dir %attr(0755, %{username}, %{groupname}) %{_localstatedir}/log/%{srcname}/
%dir %attr(0755, %{username}, %{groupname}) /run/%{srcname}/
%config %{_sysconfdir}/%{srcname}/*
%config %{_sysconfdir}/logrotate.d/*
%{_tmpfilesdir}/%{srcname}.conf
%{_unitdir}/*
%{python3_sitelib}/%{srcname}-*.egg-info/
%{python3_sitelib}/%{srcname}/
%{_bindir}/grok-*
%{_mandir}/*/*

%changelog
* Wed Sep 30 2020 Konstantin Ryabitsev <konstantin@linuxfoundation.org> - 2.0.1-1
- Update to 2.0.1

* Mon Sep 21 2020 Konstantin Ryabitsev <konstantin@linuxfoundation.org> - 2.0.0-1
- Initial 2.0.0 packaging
