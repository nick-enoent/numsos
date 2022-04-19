# numsos
Numsos is an analysis and formatting application for Ovis Meta data.

# Install package from source
./autogen.sh

mkdir build && cd build

../configure --prefix=</opt/ovis/> --with-sos=</opt/ovis> PYTHON=</path/to/python3>

make && make install

# Grafana Dashboards
grafana_dashboards subdirectory contains example Grafana Dashboards

# Packaged Dashbaords
PapiJobsTableDashboard links to papiCpuDashboard, papiCacheDashboard, and papiJobInfoDashboard
rankMemByJobDashboard links to compMinMeanMaxDashboard
