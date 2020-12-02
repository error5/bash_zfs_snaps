#!/bin/bash -xe

# TODO:
# expire remote
# restart with token
# Check space on dest
# report failures

LOCAL_HOST=$(hostname -f)						# FQDN
LOCAL_ZPATH=data2/datacenter/kvm				# Path of zfs to send to BACKUP_SERVER
ZTIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")		# ISO 8601 like format no timezone ( host time )
RETENTION=14									# Count of backups to hold, each run counts
BACKUP_SERVER="ip or hostname here"				# Destination zfs server 
BACKUP_ZPATH="data1/backup/hosts/${LOCAL_HOST}"	# Destination zfs to use on BACKUP_SERVER

# get_zfs_snapshots ( snapshot or filesystem )
function get_zfs_snapshots() {
	t_zfs_fsname=$1
	# if its a snapshot strip it off
	t_zfs_fsname=${t_zfs_fsname%%@*}
	REPLY=""
	local zfs_snapshots=$(zfs list -t snapshot -o name -Hr ${t_zfs_fsname})
	REPLY=${zfs_snapshots}
}

# get_zfs_snapshots ( snapshot or filesystem, remote zfs path, remote zfs server ) 
function get_zfs_snapshots_remote() {
	t_zfs_fsname=$1
	# if its a snapshot strip it off
	t_zfs_fsname=${t_zfs_fsname%%@*}
	t_zfs_remote_path=$2
	t_zfs_remote_server=$3
	local zfs_snapshots=$(ssh root@${t_zfs_remote_server} zfs list -t snapshot -o name -Hr ${t_zfs_remote_path}/$(basename ${t_zfs_fsname}))
	REPLY=${zfs_snapshots}
}

function get_last_snapshot_sync() {
	t_zfs_fsname=$1
	# if its a snapshot strip it off
	t_zfs_fsname=${t_zfs_fsname%%@*}

	get_zfs_snapshots ${t_zfs_fsname}
	local_snapshots=(${REPLY})
	get_zfs_snapshots_remote ${t_zfs_fsname} ${BACKUP_ZPATH} ${BACKUP_SERVER}
	remote_snapshots=(${REPLY})

	for ((i=${#local_snapshots[@]}-1; i>=0; i--)); do
		for ((j=${#remote_snapshots[@]}-1; j>=0; j--)); do
			if [ $(basename ${remote_snapshots[$j]}) == $(basename ${local_snapshots[$i]}) ] ; then
				REPLY=${local_snapshots[$i]}
				return 0
			fi
		done
	done
	echo "get_last_snapshot_sync: ERROR no match for $(basename ${remote_snapshots[$j]})"
	return 1 
}

function expire_zfs_snapshots() {
	zfs_to_expire=$1
	get_zfs_snapshots ${zfs_to_expire}

	zfs_snapshots=${REPLY}
	zfs_snapshots_a=(${zfs_snapshots})

	COUNTER=0
	zfs_snapshots_c=${#zfs_snapshots_a[@]}

	while [ ${zfs_snapshots_c} -gt ${RETENTION} ] ; do
		$(zfs destroy ${zfs_snapshots_a[${COUNTER}]})
		if [ $? != 0 ] ; then
			echo "expire_zfs_snapshots: ERROR expire snapshot failed ${zfs_snapshots_a[${COUNTER}]}"
			exit 1;
		fi

		unset zfs_snapshots_a[${COUNTER}]
		zfs_snapshots_c=${#zfs_snapshots_a[@]}
		let COUNTER=COUNTER+1
	done
	return 0
}
	

# END of function def

zfs_list=$(zfs list -d 1 -t filesystem -o name -Hr ${LOCAL_ZPATH})

## for LOCAL_ZPATH zfs without .skipbackup file snapshot with ZTIMESTAMP
for zfs in ${zfs_list}
do
	## skip parent zfs
	if [ "X${zfs}" == "X${LOCAL_ZPATH}" ] ; then 
		continue
	else
		echo ${zfs}
		zfs_path=$(zfs get -H mountpoint -o value ${zfs})
		## ignore if .skipbackup file
		echo "test: ${zfs_path}/.skipbackup"
		if [ ! -f "${zfs_path}/.skipbackup" ] ; then 
			## snapshot here
			echo "zfs snapshot ${zfs}@${ZTIMESTAMP}"
			$(zfs snapshot ${zfs}@${ZTIMESTAMP})
			if [ $? != 0 ] ; then
				echo "ZFS SNAPSHOT FAILED ON ${zfs}"
				exit 1;
			fi
		fi
	fi
done

## SEND ZFS TO BACKUP_SERVER
## GET ZFS
zfs_list=$(zfs list -d 1 -t filesystem -o name -Hr ${LOCAL_ZPATH})

## for LOCAL_ZPATH zfs without .skipbackup file 
for zfs in ${zfs_list}
do
	## skip parent zfs
	if [ "X${zfs}" == "X${LOCAL_ZPATH}" ] ; then
		continue
	else
		echo ${zfs}
		zfs_path=$(zfs get -H mountpoint -o value ${zfs})
		## ignore if .skipbackup file
		if [ ! -f "${zfs_path}/.skipbackup" ] ; then
			## snapshot here
			get_zfs_snapshots ${zfs}
			zfs_snapshots=${REPLY}
			zfs_snapshots_a=(${zfs_snapshots})

			# Does the ZFS exist on dest? 
			ssh root@${BACKUP_SERVER} zfs list ${BACKUP_ZPATH}/$(basename ${zfs})
			# then create it and flag as initial send
			if [ $? -ne 0 ] ; then 
				ssh root@${BACKUP_SERVER} zfs create -p ${BACKUP_ZPATH}/$(basename ${zfs})
				if [ $? -ne 0 ] ; then 
					exit 1;
				else
					f_remote_create=YES
				fi
			fi

			# does it exist and have snapshot we can sync from?
			get_last_snapshot_sync ${zfs}
			if [ $? -eq 0 ] ; then
				previous_zfs=$REPLY
			else
				f_remote_create=YES
			fi

			# this is where we zfs send
			if [ ${#zfs_snapshots_a[@]} -eq 1 ] || [ "X${f_remote_create}" == "XYES" ] ; then 
				echo "INITIAL SEND: ${zfs_snapshots_a[-1]}"
				zfs send -c ${zfs_snapshots_a[-1]} | \
					ssh root@${BACKUP_SERVER} zfs receive -F ${BACKUP_ZPATH}/$(basename ${zfs})
				if [ $? != 0 ] ; then
					echo "zfs send ${zfs_snapshots_a[-1]}"
					exit 1;
				fi
			elif [ ${#zfs_snapshots_a[@]} -gt 1 ] ; then 
				# find most recent zfs snapshot on both local and dest
				if [ $? -eq 0 ] ; then
					echo "INCR SEND: ${previous_zfs} to ${zfs_snapshots_a[-1]}"
					zfs send -c -i ${previous_zfs} ${zfs_snapshots_a[-1]} | \
						ssh root@${BACKUP_SERVER} zfs receive -F ${BACKUP_ZPATH}/$(basename ${zfs})

					if [ $? != 0 ] ; then
						echo " zfs send -i ${previous_zfs} ${zfs_snapshots_a[-1]}"
						exit 1;
					else 	
						# safe to expire local now. 
						expire_zfs_snapshots "${zfs}"
					fi
				else
					echo "cant send incr!"
				fi
			else
				echo "how are we here?"
			fi
		fi
	fi
done
