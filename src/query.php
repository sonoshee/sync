<?php

/* return SQL */
return <<<EOT
select
	concat('"',id,'"') as id,
	concat('"',dtb_customer_id,'"') as dtb_customer_id,
	concat('"',email,'"') as email,
	concat('"',phone_no,'"') as phone_no,
	concat('"',pref_id,'"') as pref_id,
	concat('"',city,'"') as city,
	concat('"',street,'"') as street,
	concat('"',password,'"') as password,
	concat('"',activation_key,'"') as activation_key,
	concat('"',activated,'"') as activated,
	concat('"',reset_password_date,'"') as reset_password_date,
	concat('"',reset_password_key,'"') as reset_password_key,
	concat('"',reset_password_device,'"') as reset_password_device,
	concat('"',last_login,'"') as last_login,
	concat('"',last_login_app,'"') as last_login_app,
	concat('"',created_app,'"') as created_app,
	concat('"',deleted,'"') as deleted,
	concat('"',deleted_date,'"') as deleted_date,
	concat('"',push_noti,'"') as push_noti,
	concat('"',mailflg,'"') as mailflg,
	concat('"',modified_batch,'"') as modified_batch,
	concat('"',created,'"') as created,
	concat('"',modified,'"') as modified
from
	customers
EOT;
