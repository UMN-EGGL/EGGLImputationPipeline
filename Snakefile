import os
import pandas as pd

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider
from snakemake.remote.FTP import RemoteProvider as FTPRemoteProvider


S3 = S3RemoteProvider()

HTTP = HTTPRemoteProvider()
FTP = FTPRemoteProvider()

s3_key_id = os.environ.get('AWS_ACCESS_KEY')
s3_access_key = os.environ.get('AWS_SECRET_KEY')

S3 = S3RemoteProvider(
    endpoint_url='https://s3.msi.umn.edu', 
    access_key_id=s3_key_id, 
    secret_access_key=s3_access_key
)

BUCKET = 'ec3genomes' 

samples = [ 
    'A1543', 'A2068', 'A2071','A2085', 'A4416', 'A5659', 'A5964', 'ADM9', 'ADR419', 'ADR420', 'ADR421', 'ADR422', 'ADR423', 'ADR424', 'ADR425', 'ARAB0016A', 'ARAB0017A', 'ARAB0018A', 'ARAB0019A', 
    'ARAB0020A', 'ARAB0034A', 'ARAB0035A', 'ARAB0036A', 'ARAB0132A', 'ARAB0133A', 'ARAB0134A', 'ARAB0135A', 'ARAB0136A', 'ARAB0141A', 'BAVA0183A', 'BROOKS2174', 'BROOKS3147', 'BROOKS3148', 'BW01', 
    'CLDB65664', 'CLDB65665', 'CLDB65666', 'CON15', 'CON26', 'CON3', 'CON40', 'CUTR0137A', 'CUTR0138A', 'ERR1397962', 'ERR1397965', 'ERR1527951', 'ERR1527966', 'ERR1527967', 'ERR1527968', 'ERR1527969', 
    'ERR1527970', 'ERR1527972', 'ERR1545178', 'ERR1545179', 'ERR1545180', 'ERR1545181', 'ERR1545182', 'ERR1545187', 'ERR1545188', 'ERR1545189', 'ERR1545190', 'ERR793393', 'ERR863167', 'ERR868003', 
    'ERR868004', 'ERR953413', 'ERR982786', 'ERR982794', 'FJOR0142A', 'FRMO0037A', 'FRMO0041A', 'FRMO0042A', 'FRMO0043A', 'FRMO0044A', 'FRMO0045A', 'FRMO0046A', 'FRMO0047A', 'FRMO0048A', 'FRMO0049A', 
    'FRMO0050A', 'FRMO0051A', 'FRMO0052A', 'FRMO0053A', 'FRMO0054A', 'FRMO0055A', 'FRMO0056A', 'FRMO0057A', 'FRMO0058A', 'FRMO0059A', 'FRMO0060A', 'FRMO0061A', 'FRMO0062A', 'FRMO0063A', 'FRMO0064A', 
    'FRMO0065A', 'FRMO0066A', 'FRMO0067A', 'FRMO0068A', 'FRMO0069A', 'FRTR0023A', 'FRTR0024A', 'FRTR0025A', 'FRTR0026A', 'FRTR0027A', 'FRTR0028A', 'FRTR0029A', 'FRTR0030A', 'FRTR0139A', 'FRTR0140A', 
    'HAFL0015A', 'HANO0172A', 'HF13B', 'HF13C', 'HF13D', 'HF14A', 'HF14C', 'HF15A', 'HOLS0173A', 'HOLS0174A', 'HOLS0175A', 'ICEL0143A', 'ICEL0144A', 'IT3', 'IT4', 'K743', 'K744', 'K745', 'K746', 'K747', 
    'LIPI0186A', 'LIPI0187A', 'LIPI0188A', 'LIPI0189A', 'M1005', 'M1009', 'M1012', 'M1027', 'M1048', 'M10638', 'M10639', 'M10640', 'M10641', 'M10642', 'M10643', 'M10644', 'M10645', 'M10646', 'M10647', 
    'M10648', 'M10649', 'M10652', 'M10653', 'M10656', 'M10657', 'M10658', 'M10659', 'M10661', 'M10662', 'M10663', 'M10664', 'M10665', 'M10666', 'M10667', 'M10668', 'M10672', 'M10673', 'M10675', 'M10676', 
    'M10677', 'M10678', 'M10679', 'M10681', 'M10682', 'M10683', 'M10684', 'M10990', 'M10991', 'M10992', 'M10993', 'M10994', 'M10995', 'M10996', 'M10997', 'M10998', 'M10999', 'M11000', 'M11001', 'M11002', 

    'M11003', 'M11004', 'M11005', 'M11006', 'M11007', 'M11008', 'M11009', 'M11010', 'M11011', 'M11012', 'M11013', 'M11014', 'M11015', 'M11016', 'M11017', 'M11018', 'M11019', 'M11020', 'M11021', 'M11022', 
    'M11023', 'M11024', 'M11025', 'M1433', 'M1458', 'M1503', 'M1505', 'M1506', 'M1507', 'M1510', 'M1514', 'M1516', 'M1518', 'M1519', 'M1542', 'M1545', 'M1552', 'M1555', 'M1556', 'M1557', 'M1559', 'M1561', 
    'M1563', 'M1565', 'M1568', 'M1570', 'M1571', 'M1579', 'M1581', 'M1583', 'M1930', 'M1931', 'M1932', 'M1937', 'M1939', 'M1941', 'M1942', 'M1944', 'M1946', 'M1964', 'M1966', 'M1967', 'M1968', 'M1970', 
    'M2023', 'M2025', 'M2039', 'M2044', 'M2060', 'M2068', 'M315', 'M316', 'M367', 'M369', 'M4409', 'M4410', 'M4411', 'M4418', 'M4435', 'M4437', 'M4438', 'M4440', 'M4467', 'M4472', 'M4475', 'M4476', 'M4486', 
    'M4487', 'M4488', 'M4493', 'M4513', 'M4514', 'M4515', 'M4527', 'M4537', 'M4540', 'M4541', 'M4546', 'M466', 'M467', 'M468', 'M469', 'M470', 'M472', 'M473', 'M475', 'M476', 'M477', 'M478', 'M479', 'M4809', 
    'M4814', 'M4815', 'M4826', 'M4869', 'M487', 'M4990', 'M5005', 'M5112', 'M5256', 'M5259', 'M5260', 'M5269', 'M5271', 'M5287', 'M5300', 'M5304', 'M5306', 'M6105', 'M6115', 'M6117', 'M6122', 'M6130', 
    'M6137', 'M6138', 'M6163', 'M6166', 'M6167', 'M6175', 'M6182', 'M6184', 'M6187', 'M6188', 'M6194', 'M6202', 'M6212', 'M6231', 'M6252', 'M6253', 'M6287', 'M6294', 'M6296', 'M6304', 'M6337', 'M6449', 
    'M6462', 'M6468', 'M6482', 'M6536', 'M6539', 'M6558', 'M6566', 'M6572', 'M6576', 'M6577', 'M6579', 'M6598', 'M6660', 'M6682', 'M6798', 'M6807', 'M6813', 'M6850', 'M6911', 'M6991', 'M6993', 'M7572', 
    'M7645', 'M7681', 'M7700', 'M7721', 'M7723', 'M7756', 'M7761', 'M7762', 'M7763', 'M7789', 'M7814', 'M7818', 'M7836', 'M7844', 'M8457', 'M8458', 'M8459', 'M8460', 'M8461', 'M8463', 'M8464', 'M8465', 
    'M8466', 'M8467', 'M8679', 'M8738', 'M968', 'M977', 'M989', 'M992', 'MONG0153A', 'OLDE0176A', 'OLDE0177A', 'PRZE0150A', 'PRZE0151A', 'PRZE0152A', 'PRZE0154A', 'PRZE0155A', 'PRZE0156A', 'PRZE0157A', 

    'PRZE0158A', 'PRZE0159A', 'PRZE0160A', 'PRZE0161A', 'PRZE0162A', 'RAO310', 'RAO441', 'SATR0021A', 'SATR0022A', 'SRR1046129', 'SRR1046135', 'SRR1046147', 'SRR1046151', 'SRR1048526', 'SRR1167052', 
    'SRR1167053', 'SRR1167093', 'SRR1167108', 'SRR1167109', 'SRR1167110', 'SRR1167891', 'SRR1167892', 'SRR1167893', 'SRR1564419', 'SRR1564421', 'SRR1564422', 'SRR1564423', 'SRR2102500', 'SRR2102896', 
    'SRR2103372', 'SRR2142163', 'SRR2142269', 'SRR2142311', 'SRR2142313', 'SRR3726219', 'SRR388336', 'SRR388337', 'SRR3900279', 'SRR3900280', 'SRR3900281', 'SRR3900282', 'SRR3900283', 'SRR3900284', 
    'SRR3900285', 'SRR3900286', 'SRR3900287', 'SRR3900288', 'SRR3900289', 'SRR3900290', 'SRR3900298', 'SRR3900315', 'SRR3900336', 'SRR3900337', 'SRR3900338', 'SRR3900339', 'SRR3900340', 'SRR3900341', 
    'SRR3900342', 'SRR3900343', 'SRR3900344', 'SRR3900345', 'SRR3900346', 'SRR3900347', 'SRR3900348', 'SRR3900349', 'SRR3900350', 'SRR3900351', 'SRR3900352', 'SRR3900353', 'SRR3952018', 'SRR4054238', 
    'SRR4054239', 'SRR4054240', 'SRR4054241', 'SRR4054242', 'SRR4054277', 'SRR4054279', 'SRR505867', 'SRR515202', 'SRR515203', 'SRR515204', 'SRR515205', 'SRR515206', 'SRR515208', 'SRR515209', 'SRR515211', 
    'SRR515212', 'SRR515213', 'SRR515214', 'SRR515215', 'SRR515216', 'SRR515217', 'SRR515218', 'SRR516104', 'SRR516118', 'SRR526907', 'SRR526908', 'SRR526909', 'SRR527519', 'SRR527520', 'SRR527521', 
    'SRR527527', 'SRR527528', 'SRR527799', 'SRR527800', 'SRR527801', 'SRR527802', 'SRR527803', 'SRR527804', 'SRR527805', 'SRR527806', 'SRR527807', 'SRR527809', 'SRR527825', 'SRR5755256', 'SRR6175104', 
    'SRR6175105', 'SRR6175106', 'SRR6175107', 'SRR6175108', 'SRR6175109', 'SRR6175110', 'SRR6175111', 'SRR6374293', 'SRR641364', 'SRR641365', 'SRR641366', 'SRR641367', 'STAN0149A', 'TRAK0178A', 
    'TRAK0179A',  'TWILIGHT', 'UKH3', 'WEST180A', 'WEST181A', 'YAKU0163A', 'YAKU0164A', 'YAKU0165A', 'YAKU0166A', 'YAKU0167A', 'YAKU0168A', 'YAKU0169A', 'YAKU0170A', 'YAKU0171A' 
]

contigs = [
    # Autosomes
   #'NC_009144_3','NC_009145_3'#,'NC_009146_3','NC_009147_3', #chr1-4
   #'NC_009148_3','NC_009149_3','NC_009150_3','NC_009151_3', #chr5-8
   #'NC_009152_3','NC_009153_3','NC_009154_3','NC_009155_3', #chr9-12
   #'NC_009156_3','NC_009157_3','NC_009158_3','NC_009159_3', #chr13-16
   #'NC_009160_3','NC_009161_3','NC_009162_3','NC_009163_3', #chr17-20
   #'NC_009164_3','NC_009165_3','NC_009166_3','NC_009167_3', #chr21-24
   #'NC_009168_3','NC_009169_3','NC_009170_3','NC_009171_3', #chr25-28
    'NC_009172_3', #'NC_009173_3','NC_009174_3','NC_009175_3', #chr29-32    
   #'NC_001640_1',                             # Mitochindria
   #'unplaced'                                 # Unplaced/Chrunk
] 
caller = [
    'gatk',
    'bcftools'
]
maf = [
#   'MAF01',
    'MAF005'
]

fltr = [
    'biallelic',
]

lsts = [
    'MNEc2M', 
    'MNEc670k', 
    'SNP70', 
    'SNP50'
]

vqsr = [
#   '100',
    '995',
#   '99',
]

#singularity:
#    "docker://linkageio/ec3genomes:v0.4.0"

rule all:
    input:
        # Phased
        ancient(S3.remote(expand(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED.vcf.gz',caller=caller,maf=maf,contig=contigs,fltr=fltr,vqsr=vqsr))),
        S3.remote(expand(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_imp.vcf.gz',caller=caller,maf=maf,contig=contigs,fltr=fltr,vqsr=vqsr,sample=samples),keep_local=True),

rule impute_filtered_individual:
    input:
        ref_pop     = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/ref_pop.vcf.gz'),
        sample_flt  = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_flt.vcf.gz'),
    output:
        imputed     = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_imp.vcf.gz'),
        imp_log     = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_imp.log'),
    params:
        prefix      = f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_imp',
        heap_size   = '20g',
        window_size = 1,
        overlap     = 0.01,
        nthreads    = 2,
        timeout     = '240m'
    resources:
        mem_GB      = 20
    shell:
        f'''
            timeout {{params.timeout}} java -jar $BEAGLE_JAR \
                gt={{input.sample_flt}} \
                ref={{input.ref_pop}} \
                impute=true \
                nthreads={{params.nthreads}} \
                out={{params.prefix}} \
                window={{params.window_size}} \
                overlap={{params.overlap}}
        '''

rule filter_individual_for_imputation:
    input:
        phase  = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED.vcf.gz'),
        phasei = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED.vcf.gz.csi'),
        passd  = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz'),
        passi  = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.csi'),
        lst    = S3.remote(f'{BUCKET}/data/lsts/MNEc2M.lst') 
    output:
        ref_pop     = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/ref_pop.vcf.gz'),
        sample_ref  = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_ref.vcf.gz'),
        sample_flt  = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_flt.vcf.gz'),
    shell:
        f'''
            # split out sample from reference population
            bcftools view {{input.phase}} --samples ^{{wildcards.sample}} -Oz -o {{output.ref_pop}}

            # split out the ORIGINAL sample to filter and for concordance later
            bcftools view {{input.passd}} --samples  {{wildcards.sample}} -Oz -o {{output.sample_ref}}
            bcftools index {{output.sample_ref}}

            # Down sample the single sample and remove the phase
            bcftools view {{output.sample_ref}} -R {{input.lst}} -Oz -o {{output.sample_flt}}
        '''

rule phase_VQSRPassed_vcf:
    input:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz'),
        idx = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.csi'),
    output:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED.vcf.gz'),
    params:
        prefix = f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED',
        window = 0.05,
        overlap = 0.005,
        mem = '50g'
    resources:
        mem_gb = 50
    shell:
        '''
            watchdog \
                --gt {input.vcf} \
                --out-prefix {params.prefix}  \
                --heap-size {params.mem} \
                --window {params.window} \
                --overlap {params.overlap} 
        '''

rule combine_gatk_bcftools_vcfs:
    input:
        gatk_vcf     = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz', keep_local=True),
        gatk_idx     = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.tbi', keep_local=True),
        bcftools_vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz', keep_local=True),
        bcftools_idx = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.tbi', keep_local=True),
        fna          = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',
        fai          = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',
        fdict        = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.dict'
    output:
        merged_bare                = temp(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/MERGED_BARE.vcf.gz'),
        merged_partial_annotated   = temp(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/MERGED_PARTIAL_ANNOTATED.vcf.gz'),
        merged_fully_annotated     = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz'),
        merged_fully_annotated_idx = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.idx')
    shell:
        f'''
        java -jar $GATK3_JAR \
            -T CombineVariants \
            -R {{input.fna}} \
            --variant:gatk {{input.gatk_vcf}} \
            --variant:bcftools {{input.bcftools_vcf}} \
            -genotypeMergeOptions PRIORITIZE \
            -priority gatk,bcftools | \
        bcftools annotate -x ^INFO/set,^FORMAT/GT -Oz -o {{output.merged_bare}}
        echo "Indexing {{output.merged_bare}}"
        bcftools index {{output.merged_bare}}

        echo "Adding GATK annotations to {{output.merged_bare}}" 
        bcftools annotate {{output.merged_bare}} -a {{input.gatk_vcf}} -c INFO/VQSLOD -Oz -o {{output.merged_partial_annotated}}
        bcftools index {{output.merged_partial_annotated}}

        echo "Addingt BCFTOOLS annotations to {{output.merged_partial_annotated}}"
        bcftools annotate {{output.merged_partial_annotated}} -a {{input.bcftools_vcf}} -c +INFO/VQSLOD -Oz -o {{output.merged_fully_annotated}} 
        bcftools index {{output.merged_fully_annotated}} -o {{output.merged_fully_annotated_idx}}
        ''' 

rule filter_VQSR_passed:
    input:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/ALL.vcf.gz'),
    output:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz'),
    shell:
        '''
            bcftools view -f PASS {input.vcf} -Ou |\
            bcftools annotate -x ^INFO/VQSLOD,^FORMAT/GT -O z -o {output.vcf}
        '''

rule apply_VQSR_filters_vcf:
    input:
        # VCF 
        vcf    = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz'),
        vcfidx = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz.tbi'),
        # Recalibration input
        recal     = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal'),
        recalidx  = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.idx'),
        plots     = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots'),
        plotspdf  = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots.pdf'),
        tranches  = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches'),
        tranchpdf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches.pdf'),
        # Refgen 
        fna    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna'),
        fai    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai'),
        fdict  = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict')
    output:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/ALL.vcf.gz'),
    run:
        vqsr = int(wildcards.vqsr)
        if vqsr > 100:
            vqsr = vqsr / 10
        else:
            vqsr = vqsr
        shell(f'''
            gatk ApplyVQSR \
            -R {{input.fna}} \
            -V {{input.vcf}} \
            --truth-sensitivity-filter-level {vqsr} \
            --tranches-file {{input.tranches}} \
            --recal-file {{input.recal}} \
            -mode SNP \
            -O {{output.vcf}} 
        ''')

rule recalibrate_bcftoolk_vcf:
    input:
        vcf    = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz'),
        vcfidx = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz.tbi'),
        mnec2m = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/MNEc2M.lst.vcf.gz'),
        index  = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/MNEc2M.lst.vcf.gz.tbi'),
        fna    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna'),
        fai    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai'),
        fdict  = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict')
    output:
        recal     = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal'),
        recalidx  = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.idx'),
        plots     = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots'),
        plotspdf  = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots.pdf'),
        tranches  = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches'),
        tranchpdf = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches.pdf'),
    shell:
        '''
            gatk VariantRecalibrator \
            -R {input.fna} \
            -V {input.vcf} \
            --resource:MNEc2M,known=false,training=true,truth=true,prior=15.0 {input.mnec2m} \
            -an DP \
            -an RPB \
            -an MQB \
            -an BQB \
            -an MQSB \
            -an SGB \
            -mode SNP \
            --max-gaussians 4 \
            -tranche 100.0 -tranche 99.9 -tranche 99.5 -tranche 99.0 -tranche 95.0 \
            -O {output.recal} \
            --tranches-file {output.tranches} \
            --rscript-file  {output.plots}
        '''    

rule recalibrate_gatk_vcf:
    input:
        vcf    = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz'),
        vcfidx = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz.tbi'),
        mnec2m = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/MNEc2M.lst.vcf.gz'),
        index  = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/MNEc2M.lst.vcf.gz.tbi'),
        fna    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna'),
        fai    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai'),
        fdict  = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict')
    output:
        recal     = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal'),
        recalidx  = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.idx'),
        plots     = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots'),
        plotspdf  = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots.pdf'),
        tranches  = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches'),
        tranchpdf = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches.pdf'),
    shell:
        '''
            gatk VariantRecalibrator \
            -R {input.fna} \
            -V {input.vcf} \
            --resource:MNEc2M,known=false,training=true,truth=true,prior=15.0 {input.mnec2m} \
            -an DP \
            -an QD \
            -an MQ \
            -an ReadPosRankSum \
            -an FS \
            -an SOR \
            -mode SNP \
            --max-gaussians 4 \
            -tranche 100.0 -tranche 99.9 -tranche 99.5 -tranche 99.0 -tranche 95.0 \
            -O {output.recal} \
            --tranches-file {output.tranches} \
            --rscript-file  {output.plots}
        '''    

rule subset_ALL_vcf:
    input:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz'),
        idx = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz.csi'),
        lst = S3.remote(f'{BUCKET}/data/lsts/{{lst}}.lst')
    output: 
        snps=S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{lst}}.lst.vcf.gz',keep_local=True)
    shell:
        '''
            bcftools view {input.vcf} -R {input.lst} -o {output.snps} -O z    
        '''

rule index_subset_vcf:
    input:
        vcf   = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{lst}}.vcf.gz')
    output:
        index = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{lst}}.vcf.gz.tbi',keep_local=True)
    shell:  
        '''
            gatk IndexFeatureFile -I {input.vcf} -O {output.index}
        '''

rule index_vcf:
    input:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{xxx}}.vcf.gz')
    output:
        idx  = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{xxx}}.vcf.gz.csi',keep_local=True)
    shell:
        '''
            bcftools index {input.vcf} -o {output.idx}
        '''

#-----------------------------------------------------------------------------#
#-                        Create different starting SNP sets                 -#
#-----------------------------------------------------------------------------#

rule filter_joint_bcftools_biallelic:
    input:
        gvcf = S3.remote(f'mccue-lab/ibiodatatransfer2019/joint_bcftools/{{contig}}.gvcf.gz')
    output: 
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/biallelic/{{maf}}/{{contig}}/ALL.vcf.gz')
    resources:
        disk_gb = 100,
        mem_gb = 10
    params:
        max_mem =  '10G'
    run:
        if wildcards.maf == 'MAF01':
            min_af = '0.01'
        elif wildcards.maf == 'MAF005':
            min_af = '0.005'
        shell(f'''
            bcftools view -m2 -M2 -v snps,indels --min-af {min_af} {{input.gvcf}} -Ou | \
            bcftools norm -m+any -Ou | \
            bcftools sort -m {{params.max_mem}} -O z -o {{output.vcf}} 
        ''')

rule filter_joint_gatk_biallelic:
    input:
        gvcf = S3.remote(f'mccue-lab/ibiodatatransfer2019/joint_gatk/{{contig}}.gvcf.gz')
    output: 
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/biallelic/{{maf}}/{{contig}}/ALL.vcf.gz')
    resources:
        disk_gb = 500,
        mem_gb = 50
    params:
        max_mem =  '50G'
    run:
        if wildcards.maf == 'MAF01':
            min_af = '0.01'
        elif wildcards.maf == 'MAF005':
            min_af = '0.005'
        shell(f'''
            bcftools view -m2 -M2 -v snps,indels --min-af {min_af} {{input.gvcf}} -Ou | \
            bcftools norm -m+any -Ou | \
            bcftools sort -m {{params.max_mem}} -O z -o {{output.vcf}} 
        ''')


#-----------------------------------------------------------------------------#
#-                           Create Legacy SNP lists                         -#
#-----------------------------------------------------------------------------#

rule make_snp_lsts:
    '''
        good for: LST = [MNEc2M, MNEc670k, SNP70, SNP50]
    '''
    input:
        snps = HTTP.remote('https://www.animalgenome.org/repository/pub/UMN2018.1003/{LST}.unique_remap.FINAL.csv.gz'),
        assembly = FTP.remote('ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/002/863/925/GCF_002863925.1_EquCab3.0/GCF_002863925.1_EquCab3.0_assembly_report.txt')
    output:
        lst = S3.remote(f'{BUCKET}/data/lsts/{{LST}}.lst',keep_local=True)
    run:
        import pandas as pd
        # Read in the assembly mapping
        assembly = pd.read_table(input.assembly ,comment='#', header=None) 
        chrmap =  {x:y for x,y in zip(assembly[0].values, assembly[6].values)}
        # I have NO idea why the HTTP remote returns a list ...
        df = pd.read_table(input.snps[0],sep=',')
        
        def map_chrom(x): 
            'maps the chromosome name in the Beeson et al file to the Ec3 RefSeq file'
            if x in chrmap:
                return chrmap[x]
            elif x.startswith('chrUn_ref'):
                return x.replace('chrUn_ref|','').replace('|','')
            else:
                raise ValueError
        
        # Add the refeseq chromosomes names to each  
        df['CHR'] = [map_chrom(x) for x in df.EC3_chrom]
        df['POS'] = df.EC3_pos
            
        # Output the LSTs file
        df.loc[:,['CHR','POS']].to_csv(output.lst,index=False,header=None,sep='\t')


#-----------------------------------------------------------------------------#
#-                            Move Reference Files                           -#
#-----------------------------------------------------------------------------#

rule download_reference_files:
    input:
        fna   = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna'),
        fai   = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai'),
        fdict = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict')
    output:
        fna   = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',
        fai   = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',
        fdict = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.dict'
    shell:
        '''
	cp {input.fna} {output.fna}
	cp {input.fai} {output.fai}
	cp {input.fdict} {output.fdict}
        '''

rule move_ref_genome_dict:
    input:
        fdict  = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.dict')
    output:
        fdict  = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict',keep_local=True)
    shell:
        'cp {input[0]} {output[0]}'

rule move_ref_genome:
    input:
        S3.remote('mccue-lab/ibiodatatransfer2019/GCF_002863925.1_EquCab3.0_genomic/{fna}')
    output:
        S3.remote(f'{BUCKET}/data/fna/{{fna}}',keep_local=True)
    shell:
        'cp {input[0]} {output[0]}'
