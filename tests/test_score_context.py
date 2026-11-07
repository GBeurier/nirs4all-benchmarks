"""Synthetic score-unit and task-context regressions."""
import hashlib

import pytest

from nirs4all_benchmarks.ingestion import ingest_export, validate_metadata
from nirs4all_benchmarks.ingestion.ingest import AtomicIngestionError, ingest_exports_atomic
from nirs4all_benchmarks.store import ArenaStore, Queries
from test_arena_v2 import observation, record


def source(execution, unit, *, task='b', variant='c', value=1.0):
    item = record(value, execution_id=execution)
    item['task']['task_hash'] = task * 64
    item['dataset_variant']['dataset_variant_hash'] = variant * 64
    item['scores']['observations'][0]['metric_unit'] = unit
    return item


def snapshot(store):
    data='\n'.join(store.conn.iterdump()).encode()
    blobs=sorted(str(p.relative_to(store.root)) for p in store.root.rglob('*')
                 if p.is_file() and p.suffix in {'.parquet','.json'})
    return hashlib.sha256(data).hexdigest(),blobs


@pytest.mark.parametrize('first,second', [('g','kg'), (None,'kg'), ('kg',None), ('g','g ')])
def test_unit_refusal_and_replay(tmp_path, first, second):
    with ArenaStore(tmp_path/'store') as store:
        a,b=source('a',first),source('b',second)
        assert ingest_export(store,a).ok
        before=snapshot(store)
        result=ingest_export(store,b)
        assert result.issues[0]['code']=='metric_unit_conflict'
        assert snapshot(store)==before
        assert ingest_export(store,a).status=='already_ingested'


@pytest.mark.parametrize('first,second_unit',[('g','kg'),(None,'g')])
def test_mixed_unit_export_rejected_before_writes(tmp_path,first,second_unit):
    a=source('a',first)
    other=observation(2.0, scope='test', partition='test')
    other['metric_unit']=second_unit
    a['scores']['observations'].append(other)
    assert validate_metadata(a).issues[0]['code']=='metric_unit_contradiction'
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store,a).issues[0]['code']=='metric_unit_contradiction'
        assert store.count('executions')==0


def test_same_unit_null_and_different_metric_allowed(tmp_path):
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store,source('a',None)).ok
        assert ingest_export(store,source('b',None)).ok
        item=source('c',None)
        second=observation(0.5,scope='test',partition='test')
        second.update(metric_name='r2',metric_unit='different',direction='max')
        item['scores']['observations'].append(second)
        assert ingest_export(store,item).ok


def test_r2_ranking_is_within_task(tmp_path):
    a,b=source('a','g',value=.2),source('b','g',task='e',variant='f',value=.9)
    for item in (a,b):
        item['scores']['observations'][0].update(metric_name='r2',direction='max')
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store,a).ok and ingest_export(store,b).ok
        rows=Queries(store).leaderboard(metric='r2')['rows']
        assert len(rows)==2 and [r['rank'] for r in rows]==[1,1]


def test_unknown_context_keeps_declared_unit(tmp_path):
    item=source('unknown','g')
    item['leakage_attestation']={'oof_enforced':None,'group_leakage_checked':None,
                                 'nested_cv_safe':None,'unsafe_flags':None}
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store,item).ok
        rows=Queries(store).unknown_context_rows()['rows']
        assert len(rows)==1 and rows[0]['task_hash']=='b'*64 and rows[0]['metric_unit']=='g'


def test_atomic_pack_conflict_rolls_back(tmp_path):
    with ArenaStore(tmp_path/'store') as store:
        before=snapshot(store)
        with pytest.raises(AtomicIngestionError, match='metric_unit_conflict'):
            ingest_exports_atomic(store,[source('a','g'),source('b','kg')])
        assert snapshot(store)==before


@pytest.mark.parametrize('bad_first',[True,False])
def test_existing_task_pack_rolls_back_in_either_order(tmp_path,bad_first):
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store,source('existing','g')).ok
        before=snapshot(store)
        good,bad=source('good','g'),source('bad','kg')
        with pytest.raises(AtomicIngestionError,match='metric_unit_conflict'):
            ingest_exports_atomic(store,[bad,good] if bad_first else [good,bad])
        assert snapshot(store)==before


def test_internally_mixed_legacy_replay_is_refused_before_store(tmp_path):
    from nirs4all_benchmarks.contract import ArenaRunExport
    from nirs4all_benchmarks.identity.hashing import export_hash

    a=source('a','g')
    mixed=source('a','g')
    second=observation(2.0,scope='test',partition='test')
    second['metric_unit']='kg'
    mixed['scores']['observations'].append(second)
    key=export_hash(ArenaRunExport.model_validate(mixed,strict=True).to_manifest())
    with ArenaStore(tmp_path/'store') as store:
        result=ingest_export(store,a)
        assert result.ok
        store.conn.execute('UPDATE ingestion_batches SET input_export_hash=? WHERE ingestion_batch_id=?',
                           (key,result.ingestion_batch_id))
        store.conn.commit()
        before=snapshot(store)
        assert ingest_export(store,mixed).issues[0]['code']=='metric_unit_contradiction'
        assert snapshot(store)==before


def test_recomputed_observations_keep_null_unit(tmp_path):
    from nirs4all_benchmarks.ingestion import IngestionPolicy
    from test_offline_arena import residual_bundle

    item=source('recompute','g')
    item['task']['target_unit']='target-unit'
    item['scores'].update(score_version='1.0',metric_implementation='nirs4all_benchmarks.scoring.metrics/1',
                          aggregation_policy='macro_mean')
    item['residuals']=residual_bundle()
    policy=IngestionPolicy(recompute_scores=True)
    with ArenaStore(tmp_path/'store') as store:
        result=ingest_export(store,item,policy=policy)
        assert result.ok and result.clean_report['scores_recomputed']
        rows=store.query('SELECT metric_name,metric_unit FROM metric_observations')
        assert rows and all(row['metric_unit'] is None for row in rows)
    with ArenaStore(tmp_path/'conflict') as store:
        first=source('first','g')
        first['task']['target_unit']='target-unit'
        assert ingest_export(store,first).ok
        before=snapshot(store)
        result=ingest_export(store,item,policy=policy)
        assert result.issues[0]['code']=='metric_unit_conflict'
        assert snapshot(store)==before


def test_tasks_never_pool_and_task_lens_restores_value(tmp_path):
    with ArenaStore(tmp_path/'store') as store:
        a,b=source('a','g',value=1.0),source('b','g',task='e',variant='f',value=9.0)
        for item in (a,b):
            item['pipeline']['nodes'][0]['params']={'n_components':5}
        assert ingest_export(store,a).ok and ingest_export(store,b).ok
        q=Queries(store)
        matrix=q.matrix()
        assert len(matrix['cells'])==1
        assert matrix['cells'][0]['value'] is None and matrix['cells'][0]['score_context_status']=='mixed'
        assert q.matrix(task_hash='b'*64)['cells'][0]['value']==1.0
        leader=q.leaderboard()
        assert len(leader['rows'])==2 and [r['rank'] for r in leader['rows']]==[1,1]
        assert all(r['rank_scope']=='task_metric_unit' for r in leader['rows'])
        assert q.stats()['summary']=={'n':0}
        assert q.stats(task_hash='b'*64)['summary']['mean']==1.0
        assert q.stats()['values']==[] and q.stats()['correlations']==[]
        assert q.operator_effect()['series']==[]
        assert q.operator_effect(task_hash='b'*64)['series'][0]['mean']==1.0
        assert q.pivot(group_by='model_family')['rows']==[]
        assert q.pivot(group_by='model_family',task_hash='b'*64)['rows'][0]['value']==1.0
        assert {r['task_hash'] for r in q.robustness()}=={'b'*64,'e'*64}
        assert {p['task_hash'] for p in q.parameter_effect('n_components')['points']}=={'b'*64,'e'*64}
        assert {r['task_hash'] for r in q.run_explorer()}=={'b'*64,'e'*64}
        assert {r['task_hash'] for r in q.parallel(dimensions=['model_family'])['rows']}=={'b'*64,'e'*64}
        assert all(node['score'] is None for node in q.pipeline_graph()['nodes'])
        assert all(node['score'] is None for node in q.operator_graph()['nodes'])
        assert q.composition()['rows'][0]['score'] is None
        view=q._score_view('validation',scope='cv')
        assert store.query_one('SELECT count(*) AS n FROM '+view)['n']==store.query_one(
            "SELECT count(*) AS n FROM v_ranked_metrics WHERE partition='validation' AND aggregation_level='sample' AND source_fold_id IS NULL")['n']


def test_legacy_same_key_ambiguity_is_refused(tmp_path):
    with ArenaStore(tmp_path/'store') as store:
        result=ingest_export(store,source('a','g'))
        assert result.ok
        row=store.query_one('SELECT * FROM metric_observations LIMIT 1')
        row.pop('metric_observation_id')
        row['metric_unit']='kg'
        store.insert('metric_observations',row)
        with pytest.raises(ValueError,match='stored_score_context_ambiguous'):
            Queries(store).leaderboard()


def test_unit_conflicts_casefold_and_failed_execution(tmp_path):
    a=source('a','g')
    b=source('b','g')
    b['scores']['observations'][0]['metric_name']='RMSE'
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store,a).ok and ingest_export(store,b).ok
        bad=source('failed','kg')
        bad['execution'].update(status='failed', terminal_status='failed')
        result=ingest_export(store,bad)
        assert result.issues[0]['code']=='metric_unit_conflict'
        assert 'kg' not in str(result.issues)
        assert store.count('executions')==2
    mixed=source('mixed','g')
    other=observation(3.0,scope='test',partition='test')
    other.update(metric_name='RMSE',metric_unit='kg')
    mixed['scores']['observations'].append(other)
    assert validate_metadata(mixed).issues[0]['code']=='metric_unit_contradiction'


def test_legacy_mixed_units_are_withheld_and_replay_is_first(tmp_path):
    with ArenaStore(tmp_path/'store') as store:
        a,b=source('a','g'),source('b','g')
        assert ingest_export(store,a).ok and ingest_export(store,b).ok
        store.conn.execute("UPDATE metric_observations SET metric_unit='kg' WHERE score_set_id IN "
                           "(SELECT score_set_id FROM score_sets WHERE execution_hash=?)",
                           (validate_metadata(b).resolved.execution_hash,))
        store.conn.commit()
        before=snapshot(store)
        assert ingest_export(store,a).status=='already_ingested'
        assert ingest_export(store,source('c','g')).issues[0]['code']=='metric_unit_conflict'
        assert snapshot(store)==before
        q=Queries(store)
        assert q.leaderboard()['rows']==[]
        assert q.leaderboard()['score_context']['withheld']['count']==1
        assert q.matrix()['cells'][0]['score_context_status']=='mixed'
        assert q.stats()['summary']=={'n':0}
        assert q.operator_effect()['series']==[]
        assert all(node['score'] is None for node in q.pipeline_graph()['nodes'])
        assert all(row['score'] is None for row in q.composition()['rows'])
        assert snapshot(store)==before


def test_legacy_mixed_execution_and_source_fold_join(tmp_path):
    with ArenaStore(tmp_path/'store') as store:
        item=source('a','g')
        second=observation(2.0,fold=None,label='producer-fold')
        second['metric_unit']='g'
        item['scores']['observations'].append(second)
        third=observation(3.0,fold=None,label='other-fold')
        third['metric_unit']='g'
        item['scores']['observations'].append(third)
        train=observation(4.0,fold='0',partition='train')
        train['metric_unit']='g'
        item['scores']['observations'].append(train)
        group=observation(5.0,fold=None,label='producer-fold',level='group')
        group['metric_unit']='g'
        item['scores']['observations'].append(group)
        metric=observation(.5,fold='0')
        metric.update(metric_name='r2',metric_unit='g',direction='max')
        item['scores']['observations'].append(metric)
        fold_scope=observation(6.0,fold='0',scope='fold')
        fold_scope['metric_unit']='g'
        item['scores']['observations'].append(fold_scope)
        result=ingest_export(store,item)
        assert result.ok
        q=Queries(store)
        view=q._score_view('validation',scope='cv',source_fold_id='producer-fold')
        assert store.query_one('SELECT count(*) AS n FROM '+view)['n']==store.query_one(
            "SELECT count(*) AS n FROM v_ranked_metrics WHERE partition='validation' "
            "AND aggregation_level='sample' AND source_fold_id='producer-fold'")['n']==1
        row=store.query_one("SELECT mo.* FROM metric_observations mo JOIN score_sets ss ON "
                            "ss.score_set_id=mo.score_set_id WHERE mo.fold_id='0' AND mo.metric_name='rmse' "
                            "AND mo.partition='validation' AND ss.scope='cv'")
        row.pop('metric_observation_id')
        row['fold_id']='1'
        row['metric_unit']='kg'
        store.insert('metric_observations',row)
        default_view=q._score_view('validation',scope='cv')
        assert store.query_one('SELECT count(*) AS n FROM '+default_view)['n']==store.query_one(
            "SELECT count(*) AS n FROM v_ranked_metrics WHERE partition='validation' "
            "AND aggregation_level='sample' AND source_fold_id IS NULL")['n']==4
        assert q.run_explorer()[0]['metric_value'] is None
        assert q.parallel(dimensions=['model_family'])['rows'][0]['metric'] is None
        assert q.stats()['summary']=={'n':0}
        assert {r['metric_unit'] for r in q.run_detail(result.execution_hash)['scores']}=={'g','kg'}
        assert {r['metric_unit'] for r in q.fold_scores(result.execution_hash)}=={'g','kg'}


def test_sql_aggregate_guards_withhold_before_projection(tmp_path, monkeypatch):
    with ArenaStore(tmp_path/'store') as store:
        a,b=source('a','g'),source('b','g')
        first=ingest_export(store,a)
        second=ingest_export(store,b)
        assert first.ok and second.ok
        store.conn.execute("UPDATE metric_observations SET metric_unit='kg' WHERE score_set_id IN "
                           "(SELECT score_set_id FROM score_sets WHERE execution_hash=?)", (second.execution_hash,))
        row=store.query_one('SELECT * FROM metric_observations WHERE score_set_id IN '
                            '(SELECT score_set_id FROM score_sets WHERE execution_hash=?)', (first.execution_hash,))
        row.pop('metric_observation_id')
        row['fold_id']='1'
        row['metric_unit']='kg'
        store.insert('metric_observations',row)
        captured={}
        original=store.query
        def query(sql, params=()):
            result=original(sql,params)
            if 'GROUP BY mo.run_condition_hash' in sql and 'AS mean' in sql:
                captured['leaderboard']=result
            if 'GROUP BY gb.facet_value' in sql and 'AS mean' in sql:
                captured['pivot']=result
            if 'GROUP BY mo.execution_hash' in sql and 'AS v' in sql:
                captured['stats']=result
            return result
        monkeypatch.setattr(store,'query',query)
        q=Queries(store)
        q.leaderboard()
        q.pivot(group_by='model_family')
        q.stats()
        assert captured['leaderboard'] and captured['leaderboard'][0]['mean'] is None
        assert captured['pivot'] and captured['pivot'][0]['mean'] is None
        assert any(row['eh']==first.execution_hash and row['v'] is None for row in captured['stats'])


def test_project_and_offline_task_selectors(tmp_path,caplog):
    import asyncio

    import httpx
    from nirs4all_access import ProjectPrincipal, issue_credential

    from nirs4all_benchmarks.service.app import create_app
    from nirs4all_benchmarks.service.project import create_project_app, provision_project
    root=tmp_path/'store'
    with ArenaStore(root) as store:
        first=ingest_export(store,source('a','g'))
        assert first.ok
        assert ingest_export(store,source('b','g',task='e',variant='f',value=9.0)).ok
    projects=tmp_path/'projects-root'
    projects.mkdir()
    provision_project(projects, 'a')
    provision_project(projects, 'b')
    token, credential=issue_credential(ProjectPrincipal('a','test',frozenset({'arena'}),
                                                          frozenset({'arena.read','arena.add'})))
    token_b, credential_b=issue_credential(ProjectPrincipal('b','test',frozenset({'arena'}),
                                                            frozenset({'arena.read','arena.add'})))
    project_source=source('project','g')
    project_source['dataset']['visibility']='private'
    project_source['task']['target_unit']='TARGET_UNIT'
    paths=['/api/leaderboard','/api/matrix','/api/runs','/api/operator-effect',
           '/api/parameter-effect?param=x','/api/robustness','/api/pivot?group_by=seed',
           '/api/parallel?dimensions=seed','/api/graph','/api/graph?kind=operators','/api/composition','/api/stats']
    async def exercise():
        async def tick():
            while True:
                await asyncio.sleep(0.01)
        heartbeat = asyncio.create_task(tick())
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(root)),
                                         base_url='http://arena.invalid') as client:
                assert (await client.get('/api/unknown-context')).status_code==200
                detail=(await client.get('/api/run/'+first.execution_hash)).json()
                assert detail['scores'][0]['metric_unit']=='g'
                refused=await client.post('/api/ingest',json=source('offline-conflict','UNIT_CANARY'))
                assert refused.status_code==200 and refused.json()['status']=='rejected'
                assert refused.json()['issues'][0]['code']=='metric_unit_conflict'
                assert 'UNIT_CANARY' not in refused.text
                for path in paths:
                    sep='&' if '?' in path else '?'
                    good=await client.get(path+sep+'task='+('b'*64))
                    assert good.status_code==200,(path,good.text)
                    bad=await client.get(path+sep+'task=invalid')
                    assert bad.status_code==400,(path,bad.status_code,bad.text)
            app=create_project_app(projects,[credential,credential_b])
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url='http://arena.invalid',
                                         headers={'Authorization':'Bearer '+token}) as client:
                submitted=await client.post('/api/ingest',json=project_source)
                assert submitted.status_code==200
                execution=submitted.json()['execution_hash']
                detail=(await client.get('/api/run/'+execution)).json()
                metadata=(await client.get('/api/run/'+execution+'/metadata')).json()
                assert detail['scores'][0]['metric_unit']=='g'
                assert detail['submission']['target_unit']['value']=='TARGET_UNIT'
                assert metadata['receipt']['scores']['observations'][0]['metric_unit']=='g'
                assert (await client.get('/api/unknown-context')).status_code==200
                conflict=source('conflict','UNIT_CANARY')
                conflict['dataset']['visibility']='private'
                conflict['task']['target_unit']='TARGET_UNIT'
                response=await client.post('/api/ingest',json=conflict)
                assert response.status_code==409 and response.json()['code']=='metric_unit_conflict'
                assert 'UNIT_CANARY' not in response.text
                other=await client.post('/api/ingest',json=conflict,headers={'Authorization':'Bearer '+token_b})
                assert other.status_code==200 and other.json()['status']=='committed'
                mixed=source('mixed','g')
                mixed['dataset']['visibility']='private'
                mixed['task']['target_unit']='TARGET_UNIT'
                second=observation(2.0,scope='test',partition='test')
                second['metric_unit']='UNIT_CANARY'
                mixed['scores']['observations'].append(second)
                response=await client.post('/api/ingest',json=mixed)
                assert response.status_code==422 and response.json()['code']=='metric_unit_contradiction'
                assert 'UNIT_CANARY' not in response.text
                for path in paths:
                    sep='&' if '?' in path else '?'
                    good=await client.get(path+sep+'task='+('b'*64))
                    assert good.status_code==200,(path,good.text)
                    bad=await client.get(path+sep+'task=invalid')
                    assert bad.status_code==400,(path,bad.status_code,bad.text)
        finally:
            heartbeat.cancel()
    async def tick_until_shutdown():
        while True:
            await asyncio.sleep(0.01)
    loop=asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(exercise())
        heartbeat=loop.create_task(tick_until_shutdown())
        loop.run_until_complete(loop.shutdown_default_executor())
        heartbeat.cancel()
        loop.run_until_complete(asyncio.gather(heartbeat, return_exceptions=True))
    finally:
        loop.close()
        asyncio.set_event_loop(None)
    assert 'UNIT_CANARY' not in caplog.text


def test_public_readmodel_refuses_mixed_reported_units(tmp_path):
    from nirs4all_benchmarks.site_build import _build_bundle
    from test_arena_work_review import run

    a,b=run(),run('PRIVATE','public')
    a['scores']['observations'][0]['metric_unit']='g'
    b['scores']['observations'][0]['metric_unit']='kg'
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store,a).ok
        assert ingest_export(store,b).ok
        with pytest.raises(ValueError,match='static_metric_unit_context_unsupported'):
            _build_bundle(store,Queries(store))


def test_disjoint_contexts_label_all_aggregate_surfaces(tmp_path):
    a = source('s1-a', 'g', task='a', variant='c', value=1000.0)
    b = source('s1-b', 'kg', task='e', variant='f', value=0.5)
    b['dataset']['dataset_fingerprint'] = 'd' * 64
    for item, preprocessing in ((a, 'SNV'), (b, 'MSC')):
        item['pipeline']['nodes'] = [
            {'node_id': 'pre', 'role': 'transform', 'operator': 'nirs4all.transform.' + preprocessing},
            {'node_id': 'model', 'role': 'model', 'operator': 'sklearn.cross_decomposition.PLSRegression'},
        ]
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store, a).ok and ingest_export(store, b).ok
        q = Queries(store)
        expected = {'SNV': ('a'*64, 'g', 1000.0), 'MSC': ('e'*64, 'kg', 0.5)}
        series = q.operator_effect()['series']
        assert [r['operator'].split('.')[-1] for r in series] == ['SNV', 'MSC']
        for row in series:
            task, unit, value = expected[row['operator'].split('.')[-1]]
            assert (row['task_hash'], row['metric_unit'], row['mean']) == (task, unit, value)
        scoped = q.operator_effect(task_hash='a'*64)
        assert next(r for r in scoped['series'] if r['operator'].endswith('SNV'))['mean'] == 1000.0
        pivot = q.pivot(group_by='preprocessing_op')
        assert len(pivot['rows']) == 2
        for row in pivot['rows']:
            task, unit, value = expected[row['group_value']]
            assert (row['task_hash'], row['metric_unit'], row['value']) == (task, unit, value)
        for method, key in ((q.composition, 'rows'), (q.operator_graph, 'nodes')):
            rows = method()[key]
            for short, (task, unit, value) in expected.items():
                row = next(r for r in rows if r['operator'].endswith(short))
                assert (row['task_hash'], row['metric_unit'], row['score']) == (task, unit, value)
            shared = next(r for r in rows if r['operator'].endswith('PLSRegression'))
            assert shared['score'] is None and shared['task_hash'] is None and shared['metric_unit'] is None
            assert shared['score_context_status'] == 'mixed'
        pipelines = q.pipeline_graph()['nodes']
        assert {(r['task_hash'], r['metric_unit'], r['score']) for r in pipelines} == set(expected.values())
        assert all(r['score_context_status'] == 'single' for r in pipelines)
        datasets = q.stats()['by_dataset']
        assert {(r['task_hash'], r['metric_unit'], r['mean']) for r in datasets} == set(expected.values())
        assert all(r['score_context_status'] == 'single' for r in datasets)
        assert q.stats()['summary'] == {'n': 0}


def test_homogeneous_null_unit_remains_unreported(tmp_path):
    item = source('s1-null', None, task='a', value=1.0)
    with ArenaStore(tmp_path/'store') as store:
        assert ingest_export(store, item).ok
        q = Queries(store)
        assert all((r['task_hash'], r['metric_unit']) == ('a'*64, None) for r in q.operator_effect()['series'])
        assert all((r['task_hash'], r['metric_unit']) == ('a'*64, None) for r in q.pivot(group_by='model_family')['rows'])
        assert all((r['task_hash'], r['metric_unit']) == ('a'*64, None) for r in q.composition()['rows'])
        assert all((r['task_hash'], r['metric_unit']) == ('a'*64, None) for r in q.operator_graph()['nodes'])
        assert all((r['task_hash'], r['metric_unit']) == ('a'*64, None) for r in q.pipeline_graph()['nodes'])
        assert all((r['task_hash'], r['metric_unit']) == ('a'*64, None) for r in q.stats()['by_dataset'])
