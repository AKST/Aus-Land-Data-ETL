# NSW Valuer General Data

## Getting data for this

To use the ingestion logic for this you need to bring
your own data. When I wrote this code originally, you
could actually just download it from valuer generals
website. Since then they've made it so you have to
email or call someone on their team. You can find
their contact details [here][vg_contact].

## Once you obtain the data

Once you obtain your data, create a directory at the
root of the project called `_cfg_byo_lv` and use the
file name come with. So you should just be able to
drag them in there, the file names should match the
`src_dst` [here][src_dst_eg] (and don't worry they
extension isn't supposed to be specified here).

[vg_contact]: https://www.nsw.gov.au/departments-and-agencies/department-of-planning-housing-and-infrastructure/nsw-valuer-general/contact-us
[src_dst_eg]: https://github.com/AKST/Aus-Land-Data-ETL/blob/6f035f968bf6db2259b452dee073248bd73d7c69/lib/pipeline/nsw_vg/land_values/defaults.py#L5-L102
